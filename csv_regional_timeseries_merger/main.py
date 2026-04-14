import os
from service import CSVRegionalTimeseriesMergeService 

input_directory = 'inputs'

filepaths = os.environ.get('selected_filenames', '').split(',')

print(f"_____________Merging following files: {filepaths} _____________")

merged_filename = os.environ['merged_filename']

csv_regional_timeseries_merge_service = CSVRegionalTimeseriesMergeService(
    filename=merged_filename,
    files=[f"inputs/{filepath.split('/')[-1]}" for filepath in filepaths],
    job_token=os.environ.get('ACC_JOB_TOKEN'),
    filepaths=filepaths
)
csv_regional_timeseries_merge_service()