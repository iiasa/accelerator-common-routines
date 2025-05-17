import os
from service import CsvRegionalTimeseriesVerificationService 

# selected_filenames = os.environ.get('selecteted_filenames', '')
# selected_filenames = selected_filenames.split(',')
# selected_files_ids = os.environ.get('selected_files_ids', '')
# selected_files_ids = selected_files_ids.split(',')

input_directory = 'inputs'

files = []
for dirpath, dirnames, filenames in os.walk(input_directory):
    for f in filenames:
        if f != '.gitkeep':
            full_path = os.path.join(dirpath, f)
            relative_path = os.path.relpath(full_path, start=os.getcwd())
            files.append(relative_path)

for file in files:
    
    print(f"_____________Validating file: {file} _____________")

    csv_regional_timeseries_verification_service = CsvRegionalTimeseriesVerificationService(
        dataset_template_id=os.environ.get('dataset_template_id'),
        job_token=os.environ.get('ACC_JOB_TOKEN'),
        filename=file
    )
    csv_regional_timeseries_verification_service()

    print(f"_____________DONE: Validating file: {file} _____________")