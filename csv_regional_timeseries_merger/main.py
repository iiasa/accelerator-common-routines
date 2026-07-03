import os
import socket
import json
from service import CSVRegionalTimeseriesMergeService 

input_directory = 'inputs'

filepaths = os.environ.get('selected_filenames', '').split(',')

print(f"_____________Merging following files: {filepaths} _____________")

merged_filename = os.environ['merged_filename']

files = []

for filepath in filepaths:
    rel_filepath = filepath.lstrip(os.environ.get('PROJECT_SLUG', '') + '/')
    files.append(f"/mnt/wdrv/{rel_filepath}")

def get_token():
    s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    s.settimeout(30)
    s.connect(f"/mnt/tmp/.wkube_agent/{os.environ['POD_ID']}/wagt.sock")
    payload = json.dumps({"action": "get-access-token"}).encode("utf-8")
    s.sendall(payload)
    s.shutdown(socket.SHUT_WR)
    
    resp_bytes = s.recv(4096)
    s.close()
    
    resp = json.loads(resp_bytes.decode("utf-8"))
    if resp.get("status") == "success":
        access_token = resp.get("access_token")
        return access_token
    else:
        print(f"IPC token request failed: {resp.get('error')}")

csv_regional_timeseries_merge_service = CSVRegionalTimeseriesMergeService(
    filename=merged_filename,
    files=files,
    get_job_token=get_token,
    filepaths=filepaths
)
csv_regional_timeseries_merge_service()