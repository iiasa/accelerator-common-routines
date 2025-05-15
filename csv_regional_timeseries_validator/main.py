import os
from service import CsvRegionalTimeseriesVerificationService 

selected_filenames = os.environ.get('selecteted_filenames', '')
selected_filenames = selected_filenames.split(',')
selected_files_ids = os.environ.get('selected_files_ids', '')
selected_files_ids = selected_files_ids.split(',')

for index in range(len(selected_files_ids)):
        filename = selected_filenames[index]
        bucket_object_id = selected_files_ids[index]
        
        print(f"_____________Validating file: {filename} _____________")
   
        csv_regional_timeseries_verification_service = CsvRegionalTimeseriesVerificationService(
            bucket_object_id=bucket_object_id,
            dataset_template_id=os.environ.get('dataset_template_id'),
            job_token=os.environ.get('ACC_JOB_TOKEN'),
            s3_filename=filename
        )
        csv_regional_timeseries_verification_service()

        print(f"_____________DONE: Validating file: {filename} _____________")