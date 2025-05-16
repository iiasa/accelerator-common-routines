import os
import rasterio
from rio_cogeo.cogeo import cog_translate
from rio_cogeo.profiles import cog_profiles
from accli import AjobCliService
from jsonschema import validate as jsonschema_validate
from jsonschema.exceptions import ValidationError, SchemaError




dataset_template_id = os.environ.get('dataset_template_id')

project_service = AjobCliService(
    os.environ.get('ACC_JOB_TOKEN'),
    server_url=os.environ.get('ACC_JOB_GATEWAY_SERVER'),
    verify_cert=False
)


dataset_template_details = project_service.get_dataset_template_details(dataset_template_id)
metadata_schema =  dataset_template_details.get('rules')['root']



input_directory = 'inputs'
output_directory = "outputs"

files = [f for f in os.listdir(input_directory)
         if os.path.isfile(os.path.join(input_directory, f)) and f != '.gitkeep']

for input_tif in files:
    with rasterio.open(input_tif) as src:
        # Get global metadata (for the entire file)
        global_metadata = src.tags()  # Global metadata for the whole file
        
        # Check for 'variable' and 'unit' in the global metadata
        global_variable = global_metadata.get('variable', 'Not available')
        global_unit = global_metadata.get('units', 'Not available')
        
        # Print global metadata
        print("Global Metadata:")
        print(f"  Variable: {global_variable}")
        print(f"  Unit: {global_unit}")
        print(f"Nodata: {src.nodata}")


        try:
            jsonschema_validate(
                metadata_schema
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
            
            # Check for 'variable' and 'unit' in the band metadata
            band_variable = band_metadata.get('variable', 'Not available')
            band_unit = band_metadata.get('unit', 'Not available')
            
            # Print band-specific metadata
            print(f"\nBand {band_index} Metadata:")
            print(f"  Variable: {band_variable}")
            print(f"  Unit: {band_unit}")
            
            # Prepare metadata for the band (can include global metadata if needed)
            # Here we use both global and band-specific metadata
            
            if num_bands == 1:
                band_variable = band_variable if band_variable else global_variable
                band_unit = band_unit if band_unit else global_unit
            
            additional_cog_metadata = {
                # "variable": band_variable,
                # "unit": band_unit,
                "variable": 'forest_cover',
                "unit": "adimensional"
            }
            
            # Define output file path for the individual band COG
            output_band_path = f"band_{band_index}_output_cog.tif"

            dst_profile.update({
                "dtype": "float32",  # Use the correct data type
                "nodata": 0,  # Ensure nodata is preserved
                "blockxsize": 128,
                "blockysize": 128,
            })
            
            # Apply cog_translate to convert each band to COG
            cog_translate(
                input_tif,  # Source file
                output_band_path,  # Output file path for this band
                dst_profile,
                indexes=[band_index],  # Process only the current band
                nodata=0,  # Set the nodata value for this band
                config={
                    "GDAL_NUM_THREADS": "ALL_CPUS",  # Use all CPU cores for processing
                    "GDAL_TIFF_INTERNAL_MASK": True,  # Enable internal masks for transparency
                    "GDAL_TIFF_OVR_BLOCKSIZE": "128",  # Block size for overviews
                },
                in_memory=False,  # Keep file processing on disk
                quiet=False,  # Verbose output for debugging
                additional_cog_metadata={**additional_cog_metadata}  # Merge global and band metadata
            )
            
            print(f"COG for band {band_index} saved as {output_band_path}")
