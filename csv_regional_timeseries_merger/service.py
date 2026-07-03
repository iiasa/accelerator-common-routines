import io
import time
import os
import json
import uuid
import shutil
import csv
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.csv as pa_csv
from accli import AjobCliService

def register_validation_via_ipc(
    validated_filename: str,
    dataset_template_id: int,
    validated_metadata: dict,
    validation_supporting_filenames: list
) -> bool:
    """
    Registers validation by appending the task payload to a JSON registry file
    consumed by the parent wagt agent.
    """
    pod_id = os.environ["POD_ID"]
    registry_dir = f"/mnt/tmp/.wkube_agent/{pod_id}"
    registry_path = f"{registry_dir}/post_task_registry.json"

    os.makedirs(registry_dir, exist_ok=True)

    entry = {
        "action": "register-validation-with-filename",
        "payload": {
            "validated_filename": validated_filename,
            "dataset_template_id": dataset_template_id,
            "validated_metadata": validated_metadata,
            "validation_supporting_filenames": validation_supporting_filenames
        }
    }

    if os.path.exists(registry_path):
        with open(registry_path) as f:
            registry = json.load(f)
    else:
        registry = []

    registry.append(entry)

    with open(registry_path, "w") as f:
        json.dump(registry, f, indent=2)

    return True


class CSVRegionalTimeseriesMergeService:
    def __init__(
        self,
        *,
        filename: str,
        files: list[str],
        get_job_token,
        filepaths: list[str]
    ):
        
        if not filename:
            raise ValueError("Filename for merged file is required.")


        self.project_service = lambda : AjobCliService(
            get_job_token(),
            server_url=os.environ.get('ACC_JOB_GATEWAY_SERVER'),
            verify_cert=False
        )

        self.template_rules = None

        self.output_filename = filename

        self.files = files
        self.filepaths = filepaths
    
    def check_input_files(self):
        
        if len(self.files) < 2:
            raise ValueError("Argument files should be at least two items.")
        
        first_file_type_id = self.project_service().get_filename_dataset_type(
            self.filepaths[0]
        )
        
        for filepath in self.filepaths[1:]:
            other_file_type_id = self.project_service().get_filename_dataset_type(
                filepath
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
        first_validation_details = self.project_service().get_filename_validation_details(self.filepaths[0])

        dataset_template_details = self.project_service().get_dataset_template_details(first_validation_details['dataset_template_id'])

        rules =  dataset_template_details.get('rules')

        self.rules = rules

        self.template_rules = rules
        
        time_dimension = rules['root_schema_declarations']['time_dimension']

        first_validation_metadata = first_validation_details['validation_metadata']

        for filepath in self.filepaths[1:]:
            next_validation_metadata = self.project_service().get_filename_validation_details(filepath)['validation_metadata']

            for key in first_validation_metadata:

                if f"{time_dimension.lower()}_meta" not in first_validation_metadata:
                    raise ValueError(f"Revalidate bucket object #{self.filepaths[0]}")

                if f"{time_dimension.lower()}_meta" not in next_validation_metadata:
                    raise ValueError(f"Revalidate bucket object #{filepath}")

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
        value_dimension = self.rules['root_schema_declarations']['value_dimension']
        time_dimension = self.rules['root_schema_declarations']['time_dimension']

        # Read the CSV header using standard csv reader to map column names to types
        with open(merged_filepath, 'r', encoding='utf-8-sig') as f:
            reader_csv = csv.reader(f)
            columns = next(reader_csv)

        column_types = {}
        for col in columns:
            if col == value_dimension:
                column_types[col] = pa.float32()
            elif col == time_dimension:
                column_types[col] = pa.int32()
            else:
                column_types[col] = pa.dictionary(pa.int32(), pa.string())

        convert_options = pa_csv.ConvertOptions(column_types=column_types)
        
        print(f"Streaming CSV '{merged_filepath}' to Parquet using PyArrow...")
        reader = pa_csv.open_csv(merged_filepath, convert_options=convert_options)
        
        parquet_writer = None
        rows_written = 0

        for batch in reader:
            table = pa.Table.from_batches([batch])
            if parquet_writer is None:
                parquet_writer = pq.ParquetWriter(
                    self.files[0] + '.parquet',
                    table.schema,
                    compression='snappy'
                )
            else:
                table = table.cast(parquet_writer.schema)

            parquet_writer.write_table(table)
            rows_written += len(table)
            print(f"Processed chunk of {len(table)} rows. Total written: {rows_written}")

        # Finalize writer
        if parquet_writer:
            parquet_writer.close()
        print(f"✅ Total rows written: {rows_written}")


    def __call__(self):
        self.check_input_files()

        first_downloaded_filepath = self.files[0]

        for file in self.files[1:]:

            first_file_copy = first_downloaded_filepath + ".copy"

            try:
                shutil.copyfile(first_downloaded_filepath, first_file_copy)
            except Exception as e:
                print(f"Sleeping 15 minutes for debugging...")
                time.sleep(900)
                print(f"Error occurred while copying file: {e}")

            possible_line_breaks = self.get_possible_file_line_break(first_downloaded_filepath)

            next_downloaded_filepath = file

            with open(first_file_copy, "ab") as merged_file:
                with open(next_downloaded_filepath, 'rb') as being_merged_file:
                    
                    if not set([b'\n', b'\r\n', b'\r', b'\n\r']).intersection(set(possible_line_breaks)):
                        dat = '\n'
                        merged_file.write(dat)

                    # Skip the first line of the being_merged_file
                    being_merged_file.readline()

                    # Stream copy the remaining content using shutil with a large buffer
                    shutil.copyfileobj(being_merged_file, merged_file, length=16*1024*1024)

                
        merge_only = True if os.environ.get('MERGE_ONLY') in ['True', 'true', '1', 'TRUE'] else False
        
        if merge_only:
            print('Merge complete. Validation of merge not registered in server as MERGE_ONLY is set.')
            return

        validation_metadata, dataset_template_id = self.get_merged_validated_metadata()


        self.create_associated_parquet(first_file_copy)

        # rename first_file_copy to <output_filename>.csv <first_file_copy>.parquet to <output_filename>.csv.parquet
        os.rename(first_file_copy, f"{self.output_filename}.csv")
        os.rename(f"{first_file_copy}.parquet", f"{self.output_filename}.csv.parquet")

        # move above rename files in ./outputs directory
        os.makedirs('./outputs', exist_ok=True)
        shutil.move(f"{self.output_filename}.csv", f"./outputs/{self.output_filename}.csv")
        shutil.move(f"{self.output_filename}.csv.parquet", f"./outputs/{self.output_filename}.csv.parquet")

        # Monkey patch serializer
        def monkey_patched_json_encoder_default(encoder, obj):
            if isinstance(obj, set):
                return list(obj)
            return json.JSONEncoder.default(encoder, obj)

        json.JSONEncoder.default = monkey_patched_json_encoder_default
        # Monkey patch serializer


        # register_validation_via_ipc(
        #     f"{os.environ.get('PROJECT_SLUG', '')}/job-output/{}/{self.output_filename}.csv",
        #     int(self.dataset_template_id),
        #     self.validation_metadata,
        #     [f"{self.original_filepath}.parquet"]
        # )
        print('Merge complete')


          


        