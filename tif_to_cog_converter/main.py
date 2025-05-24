import sys

_original_stdin = sys.stdin
_original_stdout = sys.stdout
_original_stderr = sys.stderr

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


# Restore if they were changed
sys.stdin = _original_stdin
sys.stdout = _original_stdout
sys.stderr = _original_stderr


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
    
    print(f"_____________Validating and converting file: {input_tif}  to cloud optimized GeoTIFF_____________")
    
    with rasterio.open(input_tif) as src:

        

        source_crs = os.environ.get('INPUT_FILE_CRS')

        if source_crs is None:
            source_crs = src.crs
        
        if source_crs is None:
            raise ValueError("CRS is neither in the file nor provided as an environment variable.")



        # Get global metadata (for the entire file)
        global_metadata = src.tags()  # Global metadata for the whole file
        
        # Check for 'variable' and 'unit' in the global metadata
        global_variable = global_metadata.get('variable', 'Not available')
        global_unit = global_metadata.get('units', 'Not available')


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
        
        # Read the number of bands in the file
        num_bands = src.count
        print(f"\nNumber of bands in the input file: {num_bands}")
        
     
        
        # Define the target CRS (Web Mercator)
        target_crs = CRS.from_epsg(3857)  # EPSG:3857 for Web Mercator

        # Process each band

        for band_index in range(1, num_bands + 1):
            # Define output file path for the individual band COG
            output_band_path = f"outputs/band_{band_index}_output_cog.tif"

            nodata_value = os.environ.get('INPUT_FILE_NODATA')
            if nodata_value is not None:
                nodata_value = float(nodata_value)
            else:
                nodata_value = src.nodata

            dst_transform, width, height = calculate_default_transform(
                    source_crs, target_crs, src.width, src.height, *src.bounds
                )
            temp_file_path = None

            # Check if reprojection is required
            if source_crs != target_crs:


                # Allocate memory for the reprojected data
                reprojected_data = src.read(band_index)
                reprojected_array = np.empty_like(reprojected_data, dtype="float32")

                # Reproject the data
                reproject(
                    source=reprojected_data,
                    destination=reprojected_array,
                    src_transform=src.transform,
                    src_crs=source_crs,
                    dst_transform=dst_transform,
                    dst_crs=target_crs,
                    resampling=Resampling.nearest,
                )
            
            

                # Apply cog_translate to convert each band to COG
                # Use the reprojected array directly in cog_translate
                with tempfile.NamedTemporaryFile(suffix=".tif", delete=False) as temp_file:
                    temp_file_path = temp_file.name
                    with rasterio.open(
                        temp_file_path,
                        "w",
                        driver="GTiff",
                        height=reprojected_array.shape[0],
                        width=reprojected_array.shape[1],
                        count=1,
                        dtype="float32",
                        crs=target_crs,
                        transform=dst_transform,
                        nodata=nodata_value,
                    ) as temp_dst:
                        temp_dst.write(reprojected_array, 1)
                        temp_dst.update_tags(**global_metadata)
                        temp_dst.update_tags(1, **src.tags(band_index))  # Add band-specific metadata


               # Create COG profile (you can adjust compression and blocksize as needed)
            dst_profile = cog_profiles.get("deflate")  # Use "deflate" compression for example

            dst_profile.update(dict(BIGTIFF="IF_SAFER"))

            dst_profile.update({
                "transform": dst_transform,
                "width": width,
                "height": height,
                "dtype": "float32",  # Use the correct data type
                "nodata": nodata_value,  # Apply nodata value from variable
                "blockxsize": 128,
                "blockysize": 128,
                "crs": target_crs,  # Properly encode CRS in the GeoTIFF
                # "TAGS": combined_metadata,  # Include both global and band-specific metadata
            })

            if temp_file_path:
                
                dst_profile.update({
                    "transform": dst_transform,
                    "width": width,
                    "height": height
                })

            # Use the temporary file as input for cog_translate
            cog_translate(
                temp_file_path if temp_file_path else input_tif,  # Use the temporary file with reprojected data
                output_band_path,  # Output file path for this band
                dst_profile,
                indexes=[1],  # Process only the first band (reprojected data)
                nodata=nodata_value,  # Explicitly set the nodata value for this band
                config={
                    "GDAL_NUM_THREADS": "ALL_CPUS",  # Use all CPU cores for processing
                    "GDAL_TIFF_INTERNAL_MASK": True,  # Enable internal masks for transparency
                    "GDAL_TIFF_OVR_BLOCKSIZE": "128",  # Block size for overviews
                },
                in_memory=False,  # Keep file processing on disk
                quiet=False,  # Verbose output for debugging
                forward_band_tags=True,  # ✅ Enables band metadata pass-through
                metadata=global_metadata,
            )

            # Clean up the temporary file
            os.remove(temp_file_path)


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
            # Monkey patch serializer


            project_service.register_validation(
                uploaded_bucket_object_id,
                dataset_template_id,
                global_metadata,
                []
            )
        
    # Delete both input and output files
    os.remove(input_tif)
    os.remove(output_band_path)

