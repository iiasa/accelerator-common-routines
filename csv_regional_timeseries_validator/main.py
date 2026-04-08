import os
from service import CsvRegionalTimeseriesVerificationService 

input_directory = 'inputs'

filepaths = os.environ.get('selected_filenames', '').split(',')


for filepath in filepaths:
    
    print(f"_____________Validating file: {file} _____________")

    csv_regional_timeseries_verification_service = CsvRegionalTimeseriesVerificationService(
        filename=f"inputs/{filepath.split('/')[-1]}",
        dataset_template_id=os.environ.get('dataset_template_id'),
        job_token=os.environ.get('ACC_JOB_TOKEN'),
        original_filepath=filepath

    )

    csv_regional_timeseries_verification_service()

    print(f"_____________DONE: Validating file: {file} _____________")