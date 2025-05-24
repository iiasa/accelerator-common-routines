import sys

import os
import json
import rasterio
from rio_cogeo.cogeo import cog_translate
from rio_cogeo.profiles import cog_profiles
from accli import AjobCliService
from jsonschema import validate as jsonschema_validate
from jsonschema.exceptions import ValidationError, SchemaError
from rasterio.warp import reproject, Resampling, calculate_default_transform
from rasterio.crs import CRS
import numpy as np
import tempfile
import subprocess



dataset_template_id = os.environ.get('dataset_template_id')

project_service = AjobCliService(
    os.environ.get('ACC_JOB_TOKEN'),
    server_url=os.environ.get('ACC_JOB_GATEWAY_SERVER'),
    verify_cert=False
)


dataset_template_details = project_service.get_dataset_template_details(dataset_template_id)
metadata_schema =  dataset_template_details.get('rules')['root']



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
    
    print(f"_____________Validating and converting file: {input_tif}  to cloud optimized GeoTIFF_____________")
    
    with rasterio.open(input_tif) as src:

        source_crs = os.environ.get('INPUT_FILE_CRS') 

        if source_crs is None:
            source_crs = src.crs
        
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
    
    for band_index in range(1, total_bands + 1):
        output_band_path = f"outputs/band_{band_index}_output_cog.tif"

        try:
            jsonschema_validate(
                metadata_schema,
                global_metadata
            )

        except SchemaError as schema_error:
            
            raise ValueError(
                f"Schema itself is not valid with template id. Template id: {dataset_template_id}. Original exception: {str(schema_error)}"
            )
        except ValidationError as validation_error:
            raise ValueError(
                f"Invalid data. Template id: {dataset_template_id}. Data: {str(validation_error)}. Original exception: {str(validation_error)}"
            )
        
        

        reprojected_raster_file = None

        if source_crs != target_crs:

            reprojected_raster_file = f"outputs/band_{band_index}_reprojected.tif"
            command = [
                "gdalwarp",
                "-s_srs", source_crs,
                "-t_srs", target_crs,
                "-r", "bilinear",              # optional: resampling method
                "-overwrite",                  # optional: overwrite output
                input_tif,
                reprojected_raster_file
            ]

            try:
                subprocess.run(command, check=True)
                print("Reprojection successful.")
            except subprocess.CalledProcessError as e:
                print("Error during reprojection:", e)



        cog_input = reprojected_raster_file if reprojected_raster_file else input_tif

        cog_cmd = [
            "gdal_translate",
            "-a_nodata", f"{nodata_value}",
            cog_input,
            output_band_path,
            "-of", "COG",
            "-co", "COMPRESS=LZW",
            "-co", "BIGTIFF=YES",
            "-co", "ADD_OVERVIEWS=YES",
            "-co", "TILING_SCHEME=GoogleMapsCompatible"
        ]

        # Run COG conversion
        subprocess.run(cog_cmd, check=True)


        

        with open(output_band_path, "rb") as file_stream:
            uploaded_bucket_object_id = project_service.add_filestream_as_job_output(
                output_band_path,
                file_stream,
            )

        # Monkey patch serializer
        def monkey_patched_json_encoder_default(encoder, obj):
            if isinstance(obj, set):
                return list(obj)
            return json.JSONEncoder.default(encoder, obj)

        json.JSONEncoder.default = monkey_patched_json_encoder_default
        
        
        project_service.register_validation(
            uploaded_bucket_object_id,
            dataset_template_id,
            global_metadata,
            []
        )

