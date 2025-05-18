import os
from service import CSVRegionalTimeseriesMergeService 

input_directory = 'inputs'

files = []
for dirpath, dirnames, filenames in os.walk(input_directory):
    for f in filenames:
        if f != '.gitkeep':
            full_path = os.path.join(dirpath, f)
            relative_path = os.path.relpath(full_path, start=os.getcwd())
            files.append(relative_path)

print(f"_____________Merging following files: {files} _____________")

merged_filename = os.environ['merged_filename']

csv_regional_timeseries_merge_service = CSVRegionalTimeseriesMergeService(
    filename=merged_filename,
    files=files,
    job_token=os.environ.get('ACC_JOB_TOKEN'),
)
csv_regional_timeseries_merge_service()