import os
from service import CSVRegionalTimeseriesMergeService 

selected_filenames = os.environ.get('selecteted_filenames', '')
selected_filenames = selected_filenames.split(',')
selected_files_ids = os.environ.get('selected_files_ids', '')
selected_files_ids = selected_files_ids.split(',')

print(f"_____________Merging following files: {selected_filenames} _____________")

merged_filename = os.environ['merged_filename']
bucket_object_id_list = selected_files_ids
csv_regional_timeseries_merge_service = CSVRegionalTimeseriesMergeService(
    filename=merged_filename,
    bucket_object_id_list=bucket_object_id_list,
    job_token=os.environ.get('ACC_JOB_TOKEN'),
)
csv_regional_timeseries_merge_service()