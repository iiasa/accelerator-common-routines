"""Convert GeoTIFFs into web friendly Cloud Optimized GeoTIFFs.

Float data is stored as Int16 with per band scale/offset by default (error at
most 0.0008% of each band's range); LOSSLESS=true keeps the exact values.
Integer data is always lossless. See README.md.

Environment variables:
    selected_filenames    comma separated `<PROJECT_SLUG>/<path>` files to convert (required in jobs)
    PROJECT_SLUG          project slug, prefix of every selected filename
    INPUT_FILE_CRS        CRS to assign when the input has none, e.g. EPSG:4326
    INPUT_FILE_NODATA     nodata value overriding the one stored in the input
    LOSSLESS              true: keep float data exact (float32 COG); default: Int16 + scale/offset
    SPLIT_BANDS           true: write one COG per band; default: one multi-band COG
    OVERVIEW_RESAMPLING   overview resampling override (nearest, average, mode, bilinear, ...)
    WEB_OPTIMIZED         true: reproject to WebMercatorQuad (changes the data); default: keep source grid
    WEB_ZOOM_LEVEL_STRATEGY  with WEB_OPTIMIZED: upper (default, no detail lost), auto or lower
    WORK_DIR              local scratch dir for the input copy and temp files (default /mnt/tmp)
    DEVELOPMENT           convert every file in ./inputs instead of selected_filenames
"""
import os
import shutil
import tempfile
import time

import numpy as np
import morecantile
import rasterio
from rasterio.crs import CRS
from rasterio.enums import ColorInterp, Resampling
from rio_cogeo.cogeo import cog_translate, cog_validate
from rio_cogeo.profiles import cog_profiles

DEVELOPMENT = os.environ.get('DEVELOPMENT', None)

# Selected files are mounted from the workspace drive; anything written
# to OUTPUT_DIRECTORY is collected by the agent as job output.
WDRV_ROOT = '/mnt/wdrv'
OUTPUT_DIRECTORY = 'outputs'
WORK_ROOT = os.environ.get('WORK_DIR', '/mnt/tmp')

TILE_SIZE = 256

# Quantized float data: -32768 is nodata, -32767..32767 hold values.
INT16_NODATA = -32768
INT16_LIMIT = 32767
INT16_STEPS = 2 * INT16_LIMIT


def env_flag(name):
    return os.environ.get(name) in ['True', 'true', '1', 'TRUE']


def get_input_files():
    """Yield (original_filepath, local_path) for every file to convert.

    In jobs the files come from the comma separated `selected_filenames`
    (`<PROJECT_SLUG>/<path>`) and are read from the mounted workspace drive.
    With DEVELOPMENT set, every file in ./inputs is used instead.
    """
    if DEVELOPMENT:
        for dirpath, _, filenames in os.walk('inputs'):
            for f in filenames:
                if f != '.gitkeep':
                    path = os.path.join(dirpath, f)
                    yield path, path
        return

    filepaths = [f.strip() for f in os.environ.get('selected_filenames', '').split(',') if f.strip()]
    if not filepaths:
        raise ValueError("No input files given: 'selected_filenames' is empty.")

    prefix = os.environ['PROJECT_SLUG'] + '/'
    for filepath in filepaths:
        if not filepath.startswith(prefix):
            raise ValueError(f"Filepath '{filepath}' does not start with the expected prefix '{prefix}'.")
        yield filepath, f"{WDRV_ROOT}/{filepath[len(prefix):]}"


def copy_to_local(input_tif, work_dir):
    """Read the input over the network exactly once, as one sequential stream.

    GDAL reads tiles with many small random reads, and the stats pass plus the
    conversion would read the file several times; both are cheap on local disk.
    """
    local_path = os.path.join(work_dir, 'source' + os.path.splitext(input_tif)[1])
    started = time.time()
    shutil.copyfile(input_tif, local_path)
    size_mb = os.path.getsize(local_path) / 1024**2
    print(f"Copied {size_mb:.1f} MB to local scratch in {time.time() - started:.1f}s")
    return local_path


def is_float_dtype(dtype):
    return np.issubdtype(np.dtype(dtype), np.floating)


def read_valid_blocks(src):
    """Yield (window, masked block of all bands) with nodata and NaN/inf masked.

    Reads all bands per block: the source is often pixel interleaved, so
    reading band by band would decompress every block once per band.
    """
    is_float = is_float_dtype(src.dtypes[0])
    for _, window in src.block_windows(1):
        block = src.read(window=window, masked=True)
        if is_float:
            block = np.ma.masked_invalid(block)
        yield window, block


def compute_statistics(src):
    """Per band count/min/max/mean/stddev of valid pixels, in one pass."""
    count = np.zeros(src.count, dtype=np.int64)
    total = np.zeros(src.count, dtype=np.float64)
    total_sq = np.zeros(src.count, dtype=np.float64)
    minimum = np.full(src.count, np.inf)
    maximum = np.full(src.count, -np.inf)

    for _, block in read_valid_blocks(src):
        for i in range(src.count):
            values = block[i].compressed().astype(np.float64)
            if values.size:
                count[i] += values.size
                total[i] += values.sum()
                total_sq[i] += np.square(values).sum()
                minimum[i] = min(minimum[i], values.min())
                maximum[i] = max(maximum[i], values.max())

    safe_count = np.maximum(count, 1)
    mean = total / safe_count
    stddev = np.sqrt(np.maximum(total_sq / safe_count - mean**2, 0.0))
    return {
        'count': count,
        'pixels': src.width * src.height,
        'minimum': minimum,
        'maximum': maximum,
        'mean': mean,
        'stddev': stddev,
    }


def write_statistics(dst, stats, scales=None, offsets=None):
    """Store statistics as GDAL band tags; forwarded into the COG (forward_band_tags).

    GDAL statistics describe the stored pixel values, so for a quantized band
    they are converted to stored units: real value = stored * scale + offset.
    """
    for i in range(dst.count):
        if not stats['count'][i]:
            print(f"Band {i + 1} has no valid pixels, no statistics written")
            continue
        scale = scales[i] if scales else 1.0
        offset = offsets[i] if offsets else 0.0
        dst.update_tags(
            i + 1,
            STATISTICS_MINIMUM=repr(float((stats['minimum'][i] - offset) / scale)),
            STATISTICS_MAXIMUM=repr(float((stats['maximum'][i] - offset) / scale)),
            STATISTICS_MEAN=repr(float((stats['mean'][i] - offset) / scale)),
            STATISTICS_STDDEV=repr(float(stats['stddev'][i] / scale)),
            STATISTICS_VALID_PERCENT=repr(round(100.0 * float(stats['count'][i]) / stats['pixels'], 4)),
        )


def quantize_to_int16(local_path, work_dir, stats):
    """Store float bands as Int16 with a per band scale/offset (lossy, bounded error).

    Each band's min..max range is spread over the 65535 Int16 steps, so the
    maximum error is (max - min) / 131068, about 0.0008% of the band's range.
    Compared with float32 this halves the size, and averaged overviews stay
    integers, so they compress as well as the full resolution data.
    """
    quantized_path = os.path.join(work_dir, 'quantized.tif')
    with rasterio.open(local_path) as src:
        scales, offsets = [], []
        for i in range(src.count):
            low, high = (stats['minimum'][i], stats['maximum'][i]) if stats['count'][i] else (0.0, 0.0)
            offsets.append(float((low + high) / 2))
            scales.append(float((high - low) / INT16_STEPS) if high > low else 1.0)

        profile = src.profile.copy()
        profile.update({
            'driver': 'GTiff',
            'dtype': 'int16',
            'nodata': INT16_NODATA,
            'tiled': True,
            'blockxsize': TILE_SIZE,
            'blockysize': TILE_SIZE,
            'compress': 'deflate',
            'predictor': 2,
            'interleave': 'band',
            'BIGTIFF': 'IF_SAFER',
        })
        with rasterio.open(quantized_path, 'w', **profile) as dst:
            scale_arr = np.array(scales)[:, None, None]
            offset_arr = np.array(offsets)[:, None, None]
            for window, block in read_valid_blocks(src):
                stored = np.round((block.astype(np.float64) - offset_arr) / scale_arr)
                stored = np.clip(stored, -INT16_LIMIT, INT16_LIMIT).filled(INT16_NODATA)
                dst.write(stored.astype(np.int16), window=window)

            dst.scales = scales
            dst.offsets = offsets
            dst.update_tags(**src.tags())
            dst.update_tags(QUANTIZATION='int16: value = stored * scale + offset')
            for i in range(src.count):
                dst.set_band_description(i + 1, src.descriptions[i])
                dst.update_tags(i + 1, **src.tags(i + 1))
                dst.update_tags(i + 1, QUANTIZATION_MAX_ERROR=repr(scales[i] / 2))
            write_statistics(dst, stats, scales, offsets)

    os.remove(local_path)  # no longer needed; frees scratch space
    return quantized_path


def prepare_source(local_path, work_dir):
    """Fix CRS/nodata, compute statistics and (unless LOSSLESS) quantize float data.

    Returns the path of the file to convert into a COG.
    """
    with rasterio.open(local_path, 'r+') as src:
        if src.crs is None:
            crs_override = os.environ.get('INPUT_FILE_CRS')
            if not crs_override:
                raise ValueError("Input has no CRS and INPUT_FILE_CRS not provided")
            src.crs = CRS.from_string(crs_override)

        nodata_override = os.environ.get('INPUT_FILE_NODATA')
        if nodata_override is not None:
            src.nodata = float(nodata_override)

        stats = compute_statistics(src)
        quantize = is_float_dtype(src.dtypes[0]) and not env_flag('LOSSLESS')
        if not quantize:
            write_statistics(src, stats)
            return local_path

    print("Quantizing float data to Int16 with per band scale/offset (set LOSSLESS=true to keep float)")
    return quantize_to_int16(local_path, work_dir, stats)


def get_overview_resampling(src):
    """Average for continuous (float) data, nearest for classes and integers.

    Averaging class codes would invent classes that do not exist, so palette
    and integer rasters keep nearest. OVERVIEW_RESAMPLING overrides the guess,
    e.g. `average` for an integer raster of counts, or `mode` for classes.
    """
    override = os.environ.get('OVERVIEW_RESAMPLING')
    if override:
        name = override.strip().lower()
        if name not in Resampling.__members__:
            raise ValueError(f"OVERVIEW_RESAMPLING '{override}' is not one of {list(Resampling.__members__)}")
        return name

    if ColorInterp.palette in src.colorinterp:
        return 'nearest'
    if is_float_dtype(src.dtypes[0]):
        return 'average'
    return 'nearest'


def get_zoom_level_strategy():
    """Which web zoom level the resolution snaps to when WEB_OPTIMIZED is set.

    `upper` (default) picks the next finer zoom level so no detail is lost;
    `auto` picks the closest one and may make the pixels coarser than the source.
    """
    strategy = os.environ.get('WEB_ZOOM_LEVEL_STRATEGY', 'upper').strip().lower()
    if strategy not in ('upper', 'lower', 'auto'):
        raise ValueError(f"WEB_ZOOM_LEVEL_STRATEGY '{strategy}' must be one of upper, lower, auto")
    return strategy


def get_cog_profile(src, band_count):
    dtype = np.dtype(src.dtypes[0])
    profile = dict(cog_profiles.get('deflate'))
    profile.update({
        'blockxsize': TILE_SIZE,
        'blockysize': TILE_SIZE,
        # Lossless: predictors only reorder bytes so DEFLATE finds more repetition.
        'predictor': 3 if is_float_dtype(dtype) else 2,
        # Band interleaved: a viewer showing one band fetches only that band's tiles.
        'interleave': 'band' if band_count > 1 else 'pixel',
        # Tiles that are entirely nodata are not written; readers fill them with
        # nodata without any request.
        'sparse_ok': True,
        'BIGTIFF': 'IF_SAFER',
    })
    return profile


def convert(src_path, output_path, indexes, work_dir, resampling):
    with rasterio.open(src_path) as src:
        profile = get_cog_profile(src, len(indexes))
        print(f"Writing {output_path}: bands {list(indexes)}, dtype {src.dtypes[0]}, "
              f"overview resampling {resampling}, interleave {profile['interleave']}")

        cog_translate(
            src,
            output_path,
            profile,
            indexes=indexes,
            nodata=src.nodata,
            overview_resampling=resampling,
            # A TMS makes rio-cogeo reproject; only do that when explicitly asked.
            tms=morecantile.tms.get("WebMercatorQuad") if env_flag('WEB_OPTIMIZED') else None,
            zoom_level_strategy=get_zoom_level_strategy(),
            resampling=resampling,
            config={
                "GDAL_NUM_THREADS": "ALL_CPUS",
                "GDAL_TIFF_OVR_BLOCKSIZE": str(TILE_SIZE),
                "CPL_TMPDIR": work_dir,
            },
            in_memory=False,
            quiet=True,
            forward_band_tags=True,
        )

    is_valid, errors, warnings = cog_validate(output_path, quiet=True)
    for warning in warnings:
        print(f"COG warning: {warning}")
    if not is_valid:
        raise ValueError(f"{output_path} is not a valid COG: {errors}")


def convert_file(input_tif):
    source_file_id = os.path.splitext(os.path.basename(input_tif))[0]
    os.makedirs(OUTPUT_DIRECTORY, exist_ok=True)

    work_dir = tempfile.mkdtemp(prefix='tif_to_cog_', dir=WORK_ROOT if os.path.isdir(WORK_ROOT) else None)
    try:
        local_src = copy_to_local(input_tif, work_dir)

        # Decided on the original data: quantized floats are stored as integers
        # but are still continuous values that should be averaged.
        with rasterio.open(local_src) as src:
            resampling = get_overview_resampling(src)
            band_count = src.count

        prepared_src = prepare_source(local_src, work_dir)

        if env_flag('SPLIT_BANDS') and band_count > 1:
            jobs = [([b], f"{source_file_id}_band_{b}.tif") for b in range(1, band_count + 1)]
        else:
            jobs = [(list(range(1, band_count + 1)), f"{source_file_id}.tif")]

        for indexes, output_name in jobs:
            work_output = os.path.join(work_dir, output_name)
            convert(prepared_src, work_output, indexes, work_dir, resampling)
            shutil.move(work_output, os.path.join(OUTPUT_DIRECTORY, output_name))
            print(f"Written {OUTPUT_DIRECTORY}/{output_name}")
    finally:
        shutil.rmtree(work_dir, ignore_errors=True)


for original_filepath, input_tif in get_input_files():
    print(f"_____________Converting file: {original_filepath} to cloud optimized GeoTIFF_____________")
    convert_file(input_tif)
    print(f"_____________DONE: Converting file: {original_filepath} _____________")
