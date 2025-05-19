import io
import os
import json
import uuid
import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd
from accli import AjobCliService


class CSVRegionalTimeseriesMergeService:
    def __init__(
        self,
        *,
        filename: str,
        files: list[int],
        job_token
    ):
        
        if not filename:
            raise ValueError("Filename for merged file is required.")


        self.project_service = AjobCliService(
            job_token,
            server_url=os.environ.get('ACC_JOB_GATEWAY_SERVER'),
            verify_cert=False
        )

        self.template_rules = None

        self.output_filename = filename

        self.files = files
    
    def check_input_files(self):
        
        if len(self.files) < 2:
            raise ValueError("Argument files should be at least two items.")
        
        first_file_type_id = self.project_service.get_filename_dataset_type(
            self.files[0][7:]
        )
        
        for file in self.files[1:]:
            other_file_type_id = self.project_service.get_filename_dataset_type(
                file[7:]
            )

            if (first_file_type_id != None) and (first_file_type_id != other_file_type_id):
                raise ValueError(
                    f"Arguments 'bucker_object_id_list' should be of same dataset template of type {first_file_type_id}."
                )
           

    def get_possible_file_line_break(self, filepath):
        breaks = []
        with open(filepath, 'rb') as fl:
            fl.seek(-1, 2)
            breaks.append(fl.read())

            fl.seek(-2, 2)
            breaks.append(fl.read())
        return breaks

        
    def get_merged_validated_metadata(self):
        first_validation_details = self.project_service.get_filename_validation_details(self.files[0][7:])

        dataset_template_details = self.project_service.get_dataset_template_details(first_validation_details['dataset_template_id'])

        rules =  dataset_template_details.get('rules')

        self.rules = rules

        self.template_rules = rules
        
        time_dimension = rules['root_schema_declarations']['time_dimension']

        first_validation_metadata = first_validation_details['validation_metadata']

        for file in self.files[1:]:
            next_validation_metadata = self.project_service.get_filename_validation_details(file[7:])['validation_metadata']

            for key in first_validation_metadata:

                if f"{time_dimension.lower()}_meta" not in first_validation_metadata:
                    raise ValueError(f"Revalidate bucket object #{self.files[0]}")

                if f"{time_dimension.lower()}_meta" not in next_validation_metadata:
                    raise ValueError(f"Revalidate bucket object #{file}")

                if key.lower() == f"{time_dimension.lower()}_meta":
                    if next_validation_metadata[key.lower()]['min_value'] < first_validation_metadata[key.lower()]['min_value']:
                        first_validation_metadata[key.lower()]['min_value'] = next_validation_metadata[key.lower()]['min_value']
                    
                    if next_validation_metadata[key.lower()]['max_value'] > first_validation_metadata[key.lower()]['max_value']:
                        first_validation_metadata[key.lower()]['max_value'] = next_validation_metadata[key.lower()]['max_value']
                elif key == 'variable-unit':
                    first_merge_candidate = first_validation_metadata[key]
                    next_merge_candidate = next_validation_metadata[key]

                    first_merge_candidate = {tuple(lst) for lst in first_merge_candidate}
                    next_merge_candidate = {tuple(lst) for lst in next_merge_candidate}

                    first_validation_metadata[key] = first_merge_candidate.union(next_merge_candidate)
                else:
                    first_validation_metadata[key] = set(first_validation_metadata[key]).union(set(next_validation_metadata[key]))
        
        return first_validation_metadata, first_validation_details['dataset_template_id']

    
    def create_associated_parquet(self, merged_filepath):
        chunksize = 100_000

        value_dimension = self.rules['root_schema_declarations']['value_dimension']
        time_dimension = self.rules['root_schema_declarations']['time_dimension']

        parquet_writer = None

        for i, chunk in enumerate(pd.read_csv(merged_filepath, chunksize=chunksize)):
            
            if value_dimension in chunk.columns:
                chunk[value_dimension] = chunk[value_dimension].astype('float32')

            if time_dimension in chunk.columns:
                chunk[time_dimension] = chunk[time_dimension].astype('int32')

            for col in chunk.columns:
                if col != value_dimension:
                    chunk[col] = chunk[col].astype('category')

            table = pa.Table.from_pandas(chunk, preserve_index=False)

            if parquet_writer is None:
                parquet_writer = pq.ParquetWriter(
                    self.files[0] + '.parquet',
                    table.schema,
                    compression='snappy'
                )

            parquet_writer.write_table(table)

        # Finalize writer
        if parquet_writer:
            parquet_writer.close()


    def __call__(self):
        self.check_input_files()

        first_downloaded_filepath = self.files[0]

        for file in self.files[1:]:

            possible_line_breaks = self.get_possible_file_line_break(first_downloaded_filepath)

            next_downloaded_filepath = file

            with open(first_downloaded_filepath, "ab") as merged_file:
                with open(next_downloaded_filepath, 'rb') as being_merged_file:
                    
                    if not set([b'\n', b'\r\n', b'\r', b'\n\r']).intersection(set(possible_line_breaks)):
                        dat = '\n'
                        merged_file.write(dat)

                    # Skip the first line of the being_merged_file
                    first_line = being_merged_file.readline()

                    while True:
                        dat = being_merged_file.read(1024**2)
                        if not dat:
                            break
                        merged_file.write(dat)

                
                
        
        if os.environ.get('MERGE_ONLY'):
            print('Merge complete. Validation of merge not registered in server as MERGE_ONLY is set.')
            return

        validation_metadata, dataset_template_id = self.get_merged_validated_metadata()


        self.create_associated_parquet(first_downloaded_filepath)

        with open(first_downloaded_filepath, "rb") as file_stream:
            uploaded_bucket_object_id = self.project_service.add_filestream_as_job_output(
                f"{self.output_filename}.csv",
                file_stream,
            )

        with open(f"{first_downloaded_filepath}.parquet", "rb") as file_stream:
            uploaded_parquet_bucket_object_id = self.project_service.add_filestream_as_validation_supporter(
                f"job-outputs/{os.environ['JOB_ID']}/{self.output_filename}.parquet",
                file_stream,
            )

        # Monkey patch serializer
        def monkey_patched_json_encoder_default(encoder, obj):
            if isinstance(obj, set):
                return list(obj)
            return json.JSONEncoder.default(encoder, obj)

        json.JSONEncoder.default = monkey_patched_json_encoder_default
        # Monkey patch serializer


        self.project_service.register_validation(
            uploaded_bucket_object_id,
            dataset_template_id,
            validation_metadata,
            [uploaded_parquet_bucket_object_id]
        )
        print('Merge complete')


          


        