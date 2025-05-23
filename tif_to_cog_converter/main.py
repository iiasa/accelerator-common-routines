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
        
        # Print global metadata
        print(">>> Global Metadata:")

        print(global_metadata)

        print(f"Nodata: {src.nodata}")


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
        
        # Create COG profile (you can adjust compression and blocksize as needed)
        dst_profile = cog_profiles.get("deflate")  # Use "deflate" compression for example

        dst_profile.update(dict(BIGTIFF="IF_SAFER"))
        
        # Process each band

        for band_index in range(1, num_bands + 1):
            
            # Get metadata (tags) for the current band
            band_metadata = src.tags(band_index)
            
            global_metadata.update(band_metadata)

            
            # Prepare metadata for the band (can include global metadata if needed)
            # Here we use both global and band-specific metadata
            
            
            # Define output file path for the individual band COG
            output_band_path = f"outputs/band_{band_index}_output_cog.tif"

            # Update the profile with CRS information
            dst_profile.update({
                "dtype": "float32",  # Use the correct data type
                "nodata": src.nodata,  # Ensure nodata is preserved
                "blockxsize": 128,
                "blockysize": 128,
                "crs": source_crs,  # Properly encode CRS in the GeoTIFF
            })


            raise ValueError(f"This is noda data: {src.nodata}")
            
            # Apply cog_translate to convert each band to COG
            cog_translate(
                input_tif,  # Source file
                output_band_path,  # Output file path for this band
                dst_profile,
                indexes=[band_index],  # Process only the current band
                nodata=src.nodata,  # Set the nodata value for this band
                config={
                    "GDAL_NUM_THREADS": "ALL_CPUS",  # Use all CPU cores for processing
                    "GDAL_TIFF_INTERNAL_MASK": True,  # Enable internal masks for transparency
                    "GDAL_TIFF_OVR_BLOCKSIZE": "128",  # Block size for overviews
                },
                in_memory=False,  # Keep file processing on disk
                quiet=False,  # Verbose output for debugging
            )
            
            print(f"COG for band {band_index} saved as {output_band_path}")


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
            print('Merge complete')

