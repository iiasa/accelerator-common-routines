import json
import os
import re
import subprocess
import csv
import uuid
import itertools
import time
import socket
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from typing import Optional
from jsonschema import validate as jsonschema_validate
from jsonschema.exceptions import ValidationError, SchemaError

from jsonschema.validators import extend, Draft202012Validator

def number_type_checker(checker, instance):
    if isinstance(instance, str):
        try:
            float(instance)  # coercible
            return True
        except ValueError:
            return False
    return isinstance(instance, (int, float))

type_checker = Draft202012Validator.TYPE_CHECKER.redefine("number", number_type_checker)
CustomValidator = extend(Draft202012Validator, type_checker=type_checker)



class CaseInsensitiveDict(dict):
    def __init__(self, *args, **kwargs):
        super().__init__()
        self.update(*args, **kwargs)

    def __setitem__(self, key, value):
        super().__setitem__(key.lower(), value)

    def __getitem__(self, key):
        return super().__getitem__(key.lower())

    def __delitem__(self, key):
        super().__delitem__(key.lower())

    def __contains__(self, key):
        return super().__contains__(key.lower())

    def get(self, key, default=None):
        return super().get(key.lower(), default)

    def pop(self, key, default=None):
        return super().pop(key.lower(), default)

    def update(self, *args, **kwargs):
        for k, v in dict(*args, **kwargs).items():
            self[k] = v

    def setdefault(self, key, default=None):
        return super().setdefault(key.lower(), default)

# Example usage:
# case_insensitive_dict = CaseInsensitiveDict({'A': 1, 'b': 2})
# print(case_insensitive_dict['a'])  # Output: 1
# print(case_insensitive_dict['B'])  # Output: 2

# case_insensitive_dict['C'] = 3
# print(case_insensitive_dict['c'])  # Output: 3


def lower_rows(iterator):
    # return itertools.chain([next(iterator).lower()], iterator)
    for item in iterator:
        yield item.lower()


def register_validation_via_ipc(
    validated_filename: str,
    dataset_template_id: int,
    validated_metadata: dict,
    validation_supporting_filenames: list
) -> bool:
    """
    Communicates with the parent wagt agent over the Unix socket to register validation.
    """
    socket_path = "/tmp/wagt.sock"
    if not os.path.exists(socket_path):
        raise FileNotFoundError(f"IPC Unix socket not found at {socket_path}. Is the agent running?")
    # 1. Structure the request matching the Go IPCRequest format
    request_payload = {
        "action": "register-validation-with-filename",
        "payload": {
            "validated_filename": validated_filename,
            "dataset_template_id": dataset_template_id,
            "validated_metadata": validated_metadata,
            "validation_supporting_filenames": validation_supporting_filenames
        }
    }
    # 2. Open Unix socket connection
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as s:
        s.connect(socket_path)
        
        # Send request payload
        payload_bytes = json.dumps(request_payload).encode('utf-8')
        s.sendall(payload_bytes)
        
        # CRITICAL: Shut down the write half of the socket to signal EOF to the Go server's io.ReadAll
        s.shutdown(socket.SHUT_WR)
        
        # Read response bytes until EOF (one-shot response)
        response_bytes = b""
        while True:
            chunk = s.recv(4096)
            if not chunk:
                break
            response_bytes += chunk
            
    # 3. Parse and check the IPCResponse
    response_data = json.loads(response_bytes.decode('utf-8'))
    if response_data.get("status") == "success":
        return response_data.get("result", False)
    else:
        raise RuntimeError(f"IPC Server Error: {response_data.get('error')}")


class CsvRegionalTimeseriesVerificationService():
    def __init__(
        self,
        *,
        filename,
        dataset_template_id,
        job_token,
        csv_fieldnames: Optional[list[str]]=None,
        ram_required=4 * 1024**3,
        disk_required=6 * 1024**3,
        cores_required=1,
        original_filepath: Optional[str]=None
    ):   

        self.dataset_template_id = dataset_template_id

        self.filename = filename

        self.ram_required = ram_required
        self.disk_required = disk_required
        self.cores_required = cores_required

        self.csv_fieldnames = csv_fieldnames
        self.original_filepath = original_filepath
        
        # Remove csv extensiopn from filename and add validation.csv. filename is relative filepath
        self.temp_validated_filepath = (
            f"{self.filename.split('.csv')[0]}_validation.csv"
        )

        self.temp_sorted_filepath = (
            f"{self.filename.split('.csv')[0]}_sorted.csv"
        )

        self.errors = dict()
    
    
    def get_map_documents(self, field_name):
        map_documents = self.rules.get(f'map_{field_name}')
        return map_documents


    def init_validation_metadata(self):
        self.validation_metadata = {
            f"{self.time_dimension}_meta": {
                "min_value": float('+inf'),
                "max_value": float('-inf')
            }
        }

    def set_csv_regional_validation_rules(self):
        import requests
        
        base_url = os.environ.get('ACC_JOB_GATEWAY_SERVER', '').rstrip('/')
        url = f"{base_url}/api/v1/ajob-cli/dataset-template-detail/{self.dataset_template_id}/"
        
        response = requests.get(url, verify=False)
        response.raise_for_status()
        dataset_template_details = response.json()
            
        self.rules =  dataset_template_details.get('rules')


        assert self.rules, \
            f"No dataset template rules found for dataset_template id: \
                {self.dataset_template_id}"
        
        self.time_dimension = self.rules['root_schema_declarations']['time_dimension']
        self.value_dimension = self.rules['root_schema_declarations']['value_dimension']

        self.unit_dimension = self.rules['root_schema_declarations']['unit_dimension']
        self.variable_dimension = self.rules['root_schema_declarations']['variable_dimension']

        self.region_dimension = self.rules['root_schema_declarations']['region_dimension']


    def preprocess_row(self, row, schema):
        """ Temporary row to validate array represented as string"""
        for field, rules in schema.get("properties", {}).items():
            if rules.get("type") == "array":
                splitter = rules.get("x-split")
                
                # Enforce: if 'x-split' exists, items must be strings
                if splitter:
                    item_type = rules.get("items", {}).get("type")
                    if item_type != "string":
                        raise ValueError(
                            f"Field '{field}' uses x-split but its items are not strings (found: {item_type})"
                        )
                    # Apply splitting
                    row[field] = row[field].split(splitter) if row.get(field) else []
                else:
                    raise ValueError(
                        f"Field '{field}' is an array but does not have 'x-split' rule defined in the schema."
        )
                
        return row
       
    
    def get_value_from__mapping_pointers(self, pointer_array, root_schema, row):
        value = None
        for pointer in pointer_array:
            if pointer.startswith('&'):
                value = root_schema[pointer[1:]]
            elif pointer.startswith('{') and pointer.endswith('}'):
                if value:
                    value = value[row[pointer[1:-1]]]
                else:
                    value = row[pointer[1:-1]]

            else:
                value = value[pointer]
        return value
    
    def validate_row_data(self, row):
        row = CaseInsensitiveDict(row)
        validation_row = self.preprocess_row(row.copy(), self.rules['root'])
        try:
            CustomValidator(
                validation_row,
                self.rules.get('root'),
            )

        except SchemaError as schema_error:
           
            raise ValueError(
                f"Schema itself is not valid with template id. Template id: {self.dataset_template_id}. Original exception: {str(schema_error)}"
            )
        except ValidationError as validation_error:
            raise ValueError(
                f"Invalid data. Template id: {self.dataset_template_id}. Data: {str(validation_error)}. Original exception: {str(validation_error)}"
            )
        

        for key in self.rules['root']['properties']:
            
            map_documents = self.get_map_documents(key)

            if map_documents:
                if type(row[key]) == list:
                    for item in row[key]:
                        if item not in map_documents:
                            raise ValueError(f"'{item}' must be one of {map_documents.keys()}" )
                elif row[key] not in map_documents:
                    raise ValueError(f"'{row[key]}' must be one of {map_documents.keys()}" )

            # TODO we will remote this whole block of metadata preparation thing.
            if self.rules['root']['properties'][key]['type'] == 'array':
                continue

            if key in [self.variable_dimension, self.unit_dimension]:
                continue

            if key == self.time_dimension:
                if float(row[key]) < self.validation_metadata[
                    f"{self.time_dimension}_meta"
                ]["min_value"]:
                    self.validation_metadata[
                        f"{self.time_dimension}_meta"
                    ]["min_value"] = float(row[key])

                if float(row[key]) > self.validation_metadata[
                    f"{self.time_dimension}_meta"
                ]["max_value"]:
                    self.validation_metadata[
                        f"{self.time_dimension}_meta"
                    ]["max_value"] = float(row[key])

            if key == self.value_dimension:
                continue


                
        
            if self.validation_metadata.get(key):
                if len(self.validation_metadata[key]) <= 1000: #limit harvest
                    self.validation_metadata[key].add(row[key])

            else:
                self.validation_metadata[key] = set([row[key]])

        if self.validation_metadata.get('variable-unit'):
            if len(self.validation_metadata['variable-unit']) <= 1000: #limit harvest
                    self.validation_metadata['variable-unit'].add((row[self.variable_dimension], row[self.unit_dimension]))
        else:
            self.validation_metadata['variable-unit'] = set([
                (row[self.variable_dimension], row[self.unit_dimension])
            ])


        extra_template_validators = self.rules.get('template_validators')

        if extra_template_validators and extra_template_validators != 'not defined':
                
            for row_key in extra_template_validators.keys():
                lhs = row[row_key]

                condition_object = extra_template_validators[row_key]

                for condition in condition_object.keys():
                
                    if condition in ['value_equals', 'is_subset_of_map']:
                    
                        rhs_value_pointer = condition_object[condition]

                        rhs = self.get_value_from__mapping_pointers(rhs_value_pointer, self.rules, validation_row)

                        if condition == 'value_equals':
                            if lhs != rhs:
                                raise ValueError(
                                    f'{lhs} in {row_key} column must be equal to {rhs}.'
                                )
                        
                        if condition == 'is_subset_of_map':
                            if not lhs in rhs:
                                raise ValueError(
                                    f'{lhs} in {row_key} column must be member of {rhs}.'
                                )
                        
                    elif condition == 'regex':
                        directive = condition_object[condition]

                        regexf = directive['regexf']
                        fcontext = directive["fcontext"]

                        resolved_fcontext = {"lhs": lhs}

                        for key in fcontext:
                            key_pointer = fcontext[key]

                            print(f"Resolving key: {key} with pointer: {key_pointer}")

                            resolved_fcontext[key] = self.get_value_from__mapping_pointers(key_pointer, self.rules, validation_row)
                        
                        pattern = regexf.format(**resolved_fcontext)

                        if not re.match(pattern, lhs.strip()):
                            raise ValueError(f"Value {lhs}  did not match pattern {pattern}")


        return validation_row, row 
          
    def get_validated_rows(self):
        with open(self.filename, encoding="utf-8-sig") as csvfile:
            reader = csv.DictReader(
                lower_rows(csvfile), 
                fieldnames=self.csv_fieldnames, 
                restkey='restkeys', 
                restval='restvals'
            )

            for row in reader:
                row.pop('restkeys', None)
                row.pop('restvals', None)

                if not any(value.strip() for value in row.values()):
                    print("Empty row detected, skipping...")
                    continue

                try:
                    validation_row, original_row = self.validate_row_data(row)
                    yield validation_row, original_row
                except Exception as err:
                    if len(self.errors) <= 50:
                        self.errors[str(err)] = str(row)

    
    def create_validated_file(self):
        # Prepare final header order
        headers = self.rules['root']['properties'].copy()

        self.validated_headers = []

        final_dimensions_order = self.rules['root_schema_declarations'].get('final_dimensions_order')

        if final_dimensions_order:
            for item in final_dimensions_order:
                if item in headers:
                    if item not in [self.time_dimension, self.value_dimension]:
                        self.validated_headers.append(item)
        else:
            raise ValueError("'final_dimensions_order' in template is required")
        
        for item in headers:
            used_headers = self.validated_headers + [self.time_dimension, self.value_dimension]
            if item not in used_headers:
                self.validated_headers.append(item)
        
        self.validated_headers = self.validated_headers + [self.time_dimension, self.value_dimension]
        # End final order preparation

        rows = self.get_validated_rows()

        passed_rows = self.create_associated_parquet(rows)

        for _ in passed_rows:
            pass
        
    
    def delete_local_file(self, filepath):
        if os.path.exists(filepath):
            os.remove(filepath)

    def create_associated_parquet(self, rows):
        chunk = []
        rows_written = 0
        chunksize = 100_000
        parquet_writer = None

        for validation_row, original_row in rows:
            chunk.append(validation_row)
            if len(chunk) >= chunksize:
                df = pd.DataFrame(chunk)
                df = self._process_dataframe(df)  # defined below
                table = self._convert_to_arrow_table(df)
                
                if parquet_writer is None:
                    parquet_writer = pq.ParquetWriter(
                        self.temp_sorted_filepath + '.parquet',
                        table.schema,
                        compression='snappy'
                    )
                else:
                    table = table.cast(parquet_writer.schema)
                parquet_writer.write_table(table)
                rows_written += len(chunk)
                print(f"Processed chunk of {len(chunk)} rows. Total written: {rows_written}")
                chunk = []

            yield validation_row, original_row

        if chunk:
            df = pd.DataFrame(chunk)
            df = self._process_dataframe(df)
            table = self._convert_to_arrow_table(df)
            if parquet_writer is None:
                parquet_writer = pq.ParquetWriter(
                    self.temp_sorted_filepath + '.parquet',
                    table.schema,
                    compression='snappy'
                )
            else:
                table = table.cast(parquet_writer.schema)
            parquet_writer.write_table(table)
            rows_written += len(chunk)

        if parquet_writer:
            parquet_writer.close()
        print(f"✅ Total rows written: {rows_written}")

    def _process_dataframe(self, df):
        for col in df.columns:
            if col == self.value_dimension:
                df[col] = df[col].astype('float32')
            elif df[col].apply(lambda x: isinstance(x, list)).all():
                # Leave list columns as-is
                continue
            elif df[col].dtype == object:
                df[col] = df[col].astype('category')
        return df

    def _convert_to_arrow_table(self, df):
        arrays = {}
        for col in df.columns:
            first_val = next((x for x in df[col] if x is not None), None)
            if isinstance(first_val, list):
                arrays[col] = pa.array(df[col], type=pa.list_(pa.string()))
            elif col == self.value_dimension:
                arrays[col] = pa.array(df[col], type=pa.float32())
            else:
                arrays[col] = pa.array(df[col])
        return pa.Table.from_arrays(
            list(arrays.values()), 
            names=list(arrays.keys())
        )

    
                
    def __call__(self):
        print("Sleeping")
        time.sleep(3600)
        self.set_csv_regional_validation_rules()

        self.init_validation_metadata()
        
        # try:
        self.create_validated_file()
        print('File validated against rules.')
        # except Exception as err:
        #     if len(self.errors) <= 50:
        #         self.errors[str(err)] = str(err)
        
        if self.errors:
            print("\n" + "!" * 80)
            print("!!! INVALID DATA DETECTED !!!".center(80))
            print("!" * 80)
            for error_msg, row_data in self.errors.items():
                print(f"\n--- ERROR ---")
                print(f"Details: {error_msg}")
                print(f"Row Data: {row_data}")
            print("\n" + "!" * 80)
            
            self.delete_local_file(self.temp_validated_filepath)
            print('Temporary validated file deleted')
            raise ValueError("Invalid data: Data does not comply with template rules.")
        
        verify_only = True if os.environ.get('VERIFY_ONLY') in ['True', 'true', '1', 'TRUE'] else False
        if verify_only:
            print('Validation complete. Validation not registered in server as VERIFY_ONLY is set.')
            return


        print("Sorting and generating CSV using Parquet...")
        import pyarrow.compute as pc
        import pyarrow.csv as pa_csv
        import time

        t_start = time.time()
        parquet_filepath = self.temp_sorted_filepath + '.parquet'
        print(f"Loading Parquet file '{parquet_filepath}' into memory...")
        table = pq.read_table(parquet_filepath)
        print(f"✅ Loaded Parquet file in {time.time() - t_start:.2f}s")

        # Select columns in the required order
        table = table.select(self.validated_headers)

        # Build a temporary table containing indices of dictionary columns to sort efficiently without high memory usage
        t_prep = time.time()
        print("Preparing temporary table for sorting by dictionary indices...")
        temp_columns = {}
        for col_name in table.schema.names:
            col = table.column(col_name)
            if isinstance(col.type, pa.DictionaryType):
                temp_columns[col_name] = col.combine_chunks().indices
            else:
                temp_columns[col_name] = col
        
        temp_table = pa.Table.from_pydict(temp_columns)
        print(f"✅ Prepared temporary table in {time.time() - t_prep:.2f}s")

        # Sort by all validated headers except the value column
        t_sort = time.time()
        print("Sorting table in memory by indices...")
        sort_keys = []
        for col in self.validated_headers[:-1]:
            sort_keys.append((col, "ascending"))

        sorted_indices = pc.sort_indices(temp_table, sort_keys=sort_keys)
        sorted_table = table.take(sorted_indices)
        print(f"✅ Sorted table in {time.time() - t_sort:.2f}s")

        # Write directly to the sorted CSV
        t_csv = time.time()
        print("Writing sorted CSV...")
        pa_csv.write_csv(sorted_table, self.temp_sorted_filepath)
        print(f"✅ Wrote sorted CSV in {time.time() - t_csv:.2f}s")
        print(f"🎉 Parquet-based sorting and CSV export completed in {time.time() - t_start:.2f}s")


        # replaced_bucket_object_id = self.replace_file_content(self.temp_sorted_filepath)

        from pathlib import Path

        Path(self.temp_sorted_filepath).replace(
            Path(self.filename)
        )
        print('File replaced')

        
        s3_parquet_filename = f"{self.original_filepath}.parquet"

        if s3_parquet_filename.startswith("/"):
            s3_parquet_filename = '/'.join(s3_parquet_filename.split("/")[2:])
        else:
            s3_parquet_filename = '/'.join(s3_parquet_filename.split("/")[1:])
        
        # with open(f"{self.temp_sorted_filepath}.parquet", "rb") as file_stream:
        #     uploaded_parquet_bucket_object_id = self.project_service.add_filestream_as_validation_supporter(
        #         s3_parquet_filename,
        #         file_stream,
        #     )

        import shutil
        shutil.copy2(
            Path(f"{self.temp_sorted_filepath}.parquet"),
            Path(f"{self.filename}.parquet")
        )

            
        # Monkey patch serializer
        def monkey_patched_json_encoder_default(encoder, obj):
            if isinstance(obj, set):
                return list(obj)
            return json.JSONEncoder.default(encoder, obj)

        json.JSONEncoder.default = monkey_patched_json_encoder_default
        # Monkey patch serializer

        register_validation_via_ipc(
            self.original_filepath,
            int(self.dataset_template_id),
            self.validation_metadata,
            [f"{self.original_filepath}.parquet"]
        )
        print('Validation complete')

   