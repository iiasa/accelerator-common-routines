# TIF to COG converter

Converts GeoTIFFs into Cloud Optimized GeoTIFFs (COGs) that load fast in web viewers.

By default, float data is stored as Int16 with a scale and offset per band. This halves the size, and the error is at most 0.0008% of each band's value range. Set `LOSSLESS=true` to keep the exact float values. Integer data is always kept exactly.

## How it runs

1. Takes the files listed in `selected_filenames` (`<PROJECT_SLUG>/<path>`, comma separated) and reads them from the workspace drive at `/mnt/wdrv/<path>`.
2. Copies each file once to local scratch (`WORK_DIR`, default `/mnt/tmp`), so the network drive is read only once, as one sequential stream.
3. Sets the CRS or nodata if overridden, then calculates per-band statistics in one pass.
4. For float data, unless `LOSSLESS=true`: converts the values to Int16 with a scale and offset per band (see [Float data](#float-data-int16-by-default-or-lossless)).
5. Writes the COG in the scratch folder, then moves it to `/mnt/output/`, where it is collected as job output. The scratch folder is always removed afterwards.

Outputs are named after the input file:

| Mode | Output |
|---|---|
| default (single- or multi-band input) | `/mnt/output/<name>.tif` with all bands |
| `SPLIT_BANDS=true`, multi-band input | `/mnt/output/<name>_band_<N>.tif`, one per band |

With `DEVELOPMENT` set, outputs go to `./outputs` instead of `/mnt/output`.

## What the output looks like

- **Data type.** Float data becomes Int16 with a scale and offset per band (default), or stays float with `LOSSLESS=true`. Integer data keeps its type and every value.
- **Compression.** DEFLATE, with predictor 2 for integers and predictor 3 for floats. A predictor only reorders bytes and changes no values.
- **256 × 256 tiles** for the full resolution and the overviews.
- **Band interleaved** for multi-band files. A viewer showing one band fetches only that band's tiles, not all bands.
- **Sparse.** Tiles that are entirely nodata are not stored; readers fill them with nodata without any request.
- **Statistics on each band**: `STATISTICS_MINIMUM/MAXIMUM/MEAN/STDDEV/VALID_PERCENT`, used by GDAL and viewers for the colour stretch.
- **Metadata carried over**: band descriptions, band and dataset tags, colour table.
- **Overviews** down to a single tile. The resampling is chosen per file (see `OVERVIEW_RESAMPLING`).
- **Checked.** Every output is checked with `cog_validate`; the job fails if the file is not a valid COG.

## Environment variables

| Variable | Default | Description |
|---|---|---|
| `selected_filenames` | — (required) | Comma separated `<PROJECT_SLUG>/<path>` files to convert. |
| `PROJECT_SLUG` | — (required) | Project slug; every selected filename must start with `<PROJECT_SLUG>/`. |
| `INPUT_FILE_CRS` | source CRS | CRS to assign when the input has none, e.g. `EPSG:4326`. Required for inputs without a CRS. |
| `INPUT_FILE_NODATA` | source nodata | Nodata value overriding the one stored in the input. |
| `LOSSLESS` | `false` | `true` keeps float data exactly as float. By default float data becomes Int16 with a scale and offset (see below). No effect on integer data. |
| `SPLIT_BANDS` | `false` | `true` writes one COG per band instead of one multi-band COG. |
| `OVERVIEW_RESAMPLING` | automatic | Overrides the overview resampling: `nearest`, `average`, `mode`, `bilinear`, `cubic`, ... |
| `WEB_OPTIMIZED` | `false` | `true` reprojects to Web Mercator during conversion (see below). |
| `WEB_ZOOM_LEVEL_STRATEGY` | `upper` | Only with `WEB_OPTIMIZED`: which zoom level's resolution to snap to (`upper`, `auto`, `lower`). |
| `WORK_DIR` | `/mnt/tmp` | Local scratch for the input copy and temp files. Needs room for about 2–3× the input size. |
| `DEVELOPMENT` | unset | Local runs: converts every file in `./inputs` instead of `selected_filenames`. |

Boolean variables accept `true`, `True`, `TRUE` or `1`.

### Float data: Int16 by default, or lossless

Float rasters (e.g. averaged soil properties) usually carry far more decimal places than the data really has. Those random low bits barely compress, and averaging them for overviews creates even more of them. By default the converter therefore stores each float band as Int16:

```
real value = stored value × scale + offset
```

- `scale` and `offset` are set per band from that band's min and max, so the band's range is spread over all 65 535 Int16 steps.
- **Maximum error** is `scale / 2`, about **0.0008% of the band's range**. For example, a sand content of 14–94 % is accurate to ±0.0006. Each band stores its exact bound in the `QUANTIZATION_MAX_ERROR` tag.
- **Nodata** (and NaN) becomes `-32768`, and the nodata areas stay exactly the same.
- **Overviews** are averaged from the Int16 values and stay integers, so they compress as well as the full-resolution data.

Measured on one overview level of the iBIOM HWSD2 soil file (22 bands, 1408 × 1131, 59% nodata):

| Mode | Size | Overviews | Max error |
|---|---|---|---|
| `LOSSLESS=true` (float32) | 51.9 MB | 29.5% of file | 0 |
| default (Int16 + scale/offset) | 28.3 MB (−45%) | 29.7% of file | 7.6e-6 × band range |

Trimming float bits instead was also tried. It saved only 28–33%, the error was 30–120× larger, and the overviews grew to over 40% of the file.

**Reading quantized files**

- GDAL-based tools apply scale/offset when you ask for it, e.g. `gdal_translate -unscale`. QGIS applies it automatically.
- With rasterio, use `value = data * src.scales[i] + src.offsets[i]`.
- With TiTiler, use `unscale=true`.
- Browser libraries such as geotiff.js / OpenLayers return the stored Int16 values. The viewer must apply scale/offset to show or query real values. The image itself looks the same, because the colour stretch can use the band statistics.
- The band statistics (`STATISTICS_*`) describe the **stored** values, as GDAL defines them. Convert them with the same formula.
- The dataset tag `QUANTIZATION` marks a file as quantized.

**Use `LOSSLESS=true` when** the exact float values matter: reference data you will compute from, round-trip archiving, values whose precision you need to keep exactly, or viewers that cannot apply scale/offset.

Also consider it when a band contains a few extreme outliers. The step size follows each band's full range, so one huge outlier makes every value in that band coarser.

The original input is never changed. If you need both a fast web version and an exact copy, run the conversion twice, once with `LOSSLESS=true`.

### Overview resampling

When `OVERVIEW_RESAMPLING` is not set:

| Input | Resampling | Why |
|---|---|---|
| has a colour table | `nearest` | class codes must not be averaged into codes that do not exist |
| integer data type | `nearest` | same, for class or code rasters |
| float data type | `average` | smooth, representative zoomed-out views of continuous values; also when stored as Int16, since the choice is made on the input's type |

Set it yourself for integer rasters that hold continuous values (e.g. `average` for elevation in whole metres), or `mode` for class rasters where the majority class is the better summary.

### Source CRS vs Web Mercator (`WEB_OPTIMIZED`)

- **Default (`WEB_OPTIMIZED` unset):** the COG keeps the source CRS and grid exactly. Use this to keep the data as published, e.g. EPSG:3035 for European grids. Viewers that reproject on the fly can display it (OpenLayers, QGIS, TiTiler).
- **`WEB_OPTIMIZED=true`:** the data is reprojected during conversion to EPSG:3857 on the WebMercatorQuad tile grid, aligned with web map zoom levels. Use this for web map clients that expect Web Mercator tiles.

Reprojecting changes the data: pixels are resampled onto a new grid with the same method as the overviews (`average` for floats, `nearest` for integers and classes). This applies even with `LOSSLESS=true`. If you need the original values as well, run the conversion twice, once with and once without `WEB_OPTIMIZED`.

The pixel size snaps to a web zoom level, controlled by `WEB_ZOOM_LEVEL_STRATEGY`:

- `upper` (default): the next finer zoom level, so no detail is lost. The file gets bigger.
- `auto`: the closest zoom level. This can be coarser than the source. For example, a 1 km grid over Europe snaps to zoom 6 (≈2.4 km pixels) and loses detail.
- `lower`: the next coarser zoom level.

## Local development

```bash
docker build -f Dockerfile.dev -t tif-to-cog-dev .
docker run --rm -v "$PWD/inputs:/code/inputs" -v "$PWD/outputs:/code/outputs" tif-to-cog-dev python main.py
```

`Dockerfile.dev` sets `DEVELOPMENT=True`, so every file in `./inputs` is converted into `./outputs`.
