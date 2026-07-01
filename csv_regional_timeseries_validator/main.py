import os
from service import CsvRegionalTimeseriesVerificationService 

input_directory = 'inputs'

filepaths = os.environ.get('selected_filenames', '').split(',')



for filepath in filepaths:

    rel_filepath = filepath.lstrip(os.environ.get('PROJECT_SLUG', '') + '/')
    
    print(f"_____________Validating file: {filepath} _____________")

    # pringt files and dirs recursively in currect working dir
    for dirpath, dirnames, filenames in os.walk("."):
        for filename in filenames:
            print(os.path.join(dirpath, filename))

    csv_regional_timeseries_verification_service = CsvRegionalTimeseriesVerificationService(
        filename=f"/mnt/wdrv/{rel_filepath}",
        # filename=f"inputs/{filepath.split('/')[-1]}",
        dataset_template_id=os.environ.get('dataset_template_id'),
        job_token=os.environ.get('ACC_JOB_TOKEN'),
        original_filepath=rel_filepath
    )

    csv_regional_timeseries_verification_service()

    print(f"_____________DONE: Validating file: {rel_filepath} _____________")