import sys
import os
import json
import rasterio
from rasterio.vrt import WarpedVRT
from rio_cogeo.cogeo import cog_translate
from rio_cogeo.profiles import cog_profiles
from accli import AjobCliService
from jsonschema import validate as jsonschema_validate
from jsonschema.exceptions import ValidationError, SchemaError
from rasterio.crs import CRS
import numpy as np
import tempfile
import subprocess

DEVELOPMENT = os.environ.get('DEVELOPMENT', None)

def get_project_service():
    project_service = AjobCliService(
        os.environ.get('ACC_JOB_TOKEN'),
        server_url=os.environ.get('ACC_JOB_GATEWAY_SERVER'),
        verify_cert=False
    )
    return project_service

def upload(output_band_path, global_metadata):
    if DEVELOPMENT:
        return
    ps = get_project_service()
    with open(output_band_path, "rb") as file_stream:
        uploaded_bucket_object_id = ps.add_filestream_as_job_output(
            output_band_path,
            file_stream,
        )

input_directory = 'inputs'
files = []
for dirpath, dirnames, filenames in os.walk(input_directory):
    for f in filenames:
        if f != '.gitkeep':
            full_path = os.path.join(dirpath, f)
            relative_path = os.path.relpath(full_path, start=os.getcwd())
            files.append(relative_path)

for input_tif in files:

    total_bands = 0
    nodata_value = None
    source_crs = None
    global_metadata = {}
    variables_metadata = []

    source_file_id = input_tif.split('.tif')[0]

    print(f"_____________Validating and converting file: {input_tif} to cloud optimized GeoTIFF_____________")

    with rasterio.open(input_tif) as src:
        # figure out source CRS
        crs_override = os.environ.get("INPUT_FILE_CRS")
        if src.crs:
            source_crs = src.crs
        elif crs_override:
            source_crs = CRS.from_string(crs_override)
        else:
            raise ValueError(f"{input_tif} has no CRS and INPUT_FILE_CRS not provided")

        total_bands = src.count
        nodata_value = os.environ.get('INPUT_FILE_NODATA')
        if nodata_value is not None:
            nodata_value = float(nodata_value)
        else:
            nodata_value = src.nodata

        global_metadata = src.tags()
        variables_metadata = [src.tags(bi) for bi in range(1, total_bands + 1)]

    for band_index in range(1, total_bands + 1):
        output_band_path = f"outputs/{source_file_id}.tif"
        if band_index > 1:
            output_band_path = f"outputs/{source_file_id}_band_{band_index}_output_cog.tif"
        os.makedirs(os.path.dirname(output_band_path), exist_ok=True)

        # compute stats memory efficiently
        min_val, max_val = None, None
        with rasterio.open(input_tif) as src:
            for _, window in src.block_windows(band_index):
                block = src.read(band_index, window=window, masked=True)
                if nodata_value is not None:
                    block = np.ma.masked_equal(block, nodata_value)
                if block.count() > 0:
                    bmin, bmax = float(block.min()), float(block.max())
                    min_val = bmin if min_val is None else min(min_val, bmin)
                    max_val = bmax if max_val is None else max(max_val, bmax)

        band_tags = {}
        band_tags.update(variables_metadata[band_index - 1])
        if min_val is not None and max_val is not None:
            band_tags.update({
                "STATISTICS_MINIMUM": str(min_val),
                "STATISTICS_MAXIMUM": str(max_val),
            })

        dst_profile = cog_profiles.get("deflate")
        dst_profile.update({
            "dtype": "float32",
            "nodata": nodata_value,
            "blockxsize": 256,
            "blockysize": 256,
            "BIGTIFF": "IF_SAFER",
        })

        # If src has no CRS, use WarpedVRT with user CRS
        with rasterio.open(input_tif) as src:
            if src.crs is None:
                with WarpedVRT(src, crs=source_crs) as vrt:
                    cog_translate(
                        vrt,
                        output_band_path,
                        dst_profile,
                        indexes=[band_index],
                        nodata=nodata_value,
                        config={
                            "GDAL_NUM_THREADS": "ALL_CPUS",
                            "GDAL_TIFF_INTERNAL_MASK": True,
                            "GDAL_TIFF_OVR_BLOCKSIZE": "256",
                        },
                        in_memory=False,
                        quiet=False,
                        forward_band_tags=True,
                        band_tags={band_index: band_tags},
                        web_optimized=True
                    )
            else:
                cog_translate(
                    src,
                    output_band_path,
                    dst_profile,
                    indexes=[band_index],
                    nodata=nodata_value,
                    config={
                        "GDAL_NUM_THREADS": "ALL_CPUS",
                        "GDAL_TIFF_INTERNAL_MASK": True,
                        "GDAL_TIFF_OVR_BLOCKSIZE": "256",
                    },
                    in_memory=False,
                    quiet=False,
                    forward_band_tags=True,
                    band_tags={band_index: band_tags},
                    web_optimized=True
                )

        upload(output_band_path, global_metadata)
        if not DEVELOPMENT:
            os.remove(output_band_path)
