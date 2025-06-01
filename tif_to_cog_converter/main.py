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

    source_file_id = '-'.join(input_tif.split('.tif')[0].split('/'))
    
    reprojected_raster_file = None

    # if source_crs != target_crs:

    #     reprojected_raster_file = f"outputs/{source_file_id}-reprojected.tif"
    #     command = [
    #         "gdalwarp",
    #         "-s_srs", source_crs,
    #         "-t_srs", target_crs,
    #         "-r", "near",              # optional: resampling method
    #         "-overwrite",                  # optional: overwrite output
    #         input_tif,
    #         reprojected_raster_file
    #     ]

    #     try:
    #         subprocess.run(command, check=True)
    #         print("Reprojection successful.")
    #     except subprocess.CalledProcessError as e:
    #         print("Error during reprojection:", e)

    
    for band_index in range(1, total_bands + 1):
        
        output_band_path = f"outputs/{source_file_id}_band_{band_index}_output_cog.tif"

        try:
            jsonschema_validate(
                global_metadata,
                metadata_schema
            )

        except SchemaError as schema_error:
            
            raise ValueError(
                f"Schema itself is not valid with template id. Template id: {dataset_template_id}. Original exception: {str(schema_error)}"
            )
        except  ValidationError as validation_error:
            raise ValueError(
                f"Invalid data. Template id: {dataset_template_id}. Data: {str(validation_error)}. Original exception: {str(validation_error)}"
            )
        

        cog_input = reprojected_raster_file if reprojected_raster_file else input_tif

        dst_profile = cog_profiles.get("deflate")


        dst_profile.update({
            "dtype": "float32",  # Use the correct data type
            "nodata": nodata_value,  # Ensure nodata is preserved
            "blockxsize": 128,
            "blockysize": 128,
            "crs": source_crs,  # Properly encode CRS in the GeoTIFF
            "BIGTIFF":"IF_SAFER"
        })

        cog_translate(
            input_tif,
            output_band_path,
            dst_profile,
            indexes=[band_index],  # Process only the current band
            nodata=nodata_value,  # Set the nodata value for this band
            config={
                "GDAL_NUM_THREADS": "ALL_CPUS",  # Use all CPU cores for processing
                "GDAL_TIFF_INTERNAL_MASK": True,  # Enable internal masks for transparency
                "GDAL_TIFF_OVR_BLOCKSIZE": "128",  # Block size for overviews
            },
            in_memory=False,  # Keep file processing on disk
            quiet=False,
            forward_band_tags=True
        )


        

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

        os.remove(output_band_path)

    if reprojected_raster_file:
        os.remove(reprojected_raster_file)
        reprojected_raster_file = None
    

