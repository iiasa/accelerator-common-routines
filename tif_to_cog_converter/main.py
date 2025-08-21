import sys

import os
import json
import rasterio
from rasterio.warp import calculate_default_transform, reproject, Resampling
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

dataset_template_id = os.environ.get('dataset_template_id')


def get_project_service():

    project_service = AjobCliService(
        os.environ.get('ACC_JOB_TOKEN'),
        server_url=os.environ.get('ACC_JOB_GATEWAY_SERVER'),
        verify_cert=False
    )

    return project_service

def get_metadata_schema():

    if DEVELOPMENT:
        return {
                "type": "object",
                "required": [],
                "properties": {},
                "additionalProperties": True
            }

    ps = get_project_service()
    dataset_template_details = ps.get_dataset_template_details(dataset_template_id)
    metadata_schema =  dataset_template_details.get('rules')['root']

    return metadata_schema

def upload_and_register_output(output_band_path, global_metadata):

    if DEVELOPMENT:
        return

    ps = get_project_service()
    with open(output_band_path, "rb") as file_stream:
        uploaded_bucket_object_id = ps.add_filestream_as_job_output(
            output_band_path,
            file_stream,
        )

        # Monkey patch serializer
        def monkey_patched_json_encoder_default(encoder, obj):
            if isinstance(obj, set):
                return list(obj)
            return json.JSONEncoder.default(encoder, obj)

        json.JSONEncoder.default = monkey_patched_json_encoder_default
        
        ps.register_validation(
            uploaded_bucket_object_id,
            dataset_template_id,
            global_metadata,
            []
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
    target_crs = 'EPSG:3857'
    global_metadata = {}
    variables_metadata = []

    source_file_id = '-'.join(input_tif.split('.tif')[0].split('/')) 
    reprojected_raster_file = None
    
    print(f"_____________Validating and converting file: {input_tif}  to cloud optimized GeoTIFF_____________")
    
    with rasterio.open(input_tif) as src:

        source_crs = os.environ.get('INPUT_FILE_CRS') 

        if source_crs is None:
            source_crs = src.crs.to_epsg()
        
        if source_crs is None:
            raise ValueError("CRS is neither in the file nor provided as an environment variable.")

        
        total_bands = src.count
        nodata_value = os.environ.get('INPUT_FILE_NODATA')
        if nodata_value is not None:
            nodata_value = float(nodata_value)
        else:
            nodata_value = src.nodata


        global_metadata = src.tags()
        variables_metadata = [src.tags(bi) for bi in range(1, total_bands + 1)]



        if source_crs != target_crs:

            reprojected_raster_file = f"outputs/{source_file_id}-reprojected.tif"
    
            transform, width, height = calculate_default_transform(
                source_crs, target_crs, src.width, src.height, *src.bounds
            )

            kwargs = src.meta.copy()
            kwargs.update({
                'crs': target_crs,
                'transform': transform,
                'width': width,
                'height': height,
                'compress': 'LZW', # Optional: Add compression to the temporary file
                'tiled': True       # Optional: Write temporary file as tiled
            })

            
            
            with rasterio.open(reprojected_raster_file, 'w', **kwargs) as dst:
                for i in range(1, src.count + 1):
                    data = src.read(i)
                    reproject(
                        source=data,
                        destination=rasterio.band(dst, i),
                        src_transform=src.transform,
                        src_crs=source_crs,
                        dst_transform=transform,
                        dst_crs=target_crs,
                        resampling=Resampling.nearest
                    )

                    dst.set_band_description(i, src.descriptions[i - 1])

                    if src.units:
                        dst.set_band_unit(i, src.units[i - 1])
                    
                    # Fix: Ensure band tags are strings
                    dst.update_tags(i, **{k: str(v) for k, v in src.tags(i).items()})

                # Fix: Ensure dataset-level tags are strings
                dst.update_tags(**{k: str(v) for k, v in src.tags().items()})

        
    for band_index in range(1, total_bands + 1):
        
        output_band_path = f"outputs/{source_file_id}_band_{band_index}_output_cog.tif"

        cog_input = reprojected_raster_file if reprojected_raster_file else input_tif

        dst_profile = cog_profiles.get("deflate")

        dst_profile.update({
            "dtype": "float32",
            "nodata": nodata_value,
            "blockxsize": 128,
            "blockysize": 128,
            "crs": target_crs,
            "BIGTIFF":"IF_SAFER"
        })

        cog_translate(
            cog_input,
            output_band_path,
            dst_profile,
            indexes=[band_index],
            nodata=nodata_value,
            config={
                "GDAL_NUM_THREADS": "ALL_CPUS",
                "GDAL_TIFF_INTERNAL_MASK": True,
                "GDAL_TIFF_OVR_BLOCKSIZE": "128",
            },
            in_memory=False,
            quiet=False,
            forward_band_tags=True
        )

        upload_and_register_output(output_band_path, global_metadata)        

        if not DEVELOPMENT:
            os.remove(output_band_path)

    if reprojected_raster_file:
        if (not DEVELOPMENT):
            os.remove(reprojected_raster_file)
        reprojected_raster_file = None
    

