from accli import WKubeTask

regional_validator = WKubeTask(
    name="Buffered Python Test",
    job_folder='./',
    # repo_url="https://github.com/iiasa/accelerator-common-routines.git",
    # repo_branch="master",
    docker_filename="Dockerfile.wkube",
    command="python main.py",
    input_mappings="selected_files:/code/inputs/",
    required_cores=1,
    required_ram=1024*1024*1024,
    required_storage_local=1024*1024*1024,
    required_storage_workflow=1024,
    timeout=3600,
    conf={
        "dataset_template_id": 20,
        "VERIFY_ONLY":'True',
        "input_mappings": "acc://brightspace/user_uploads/Agmemod4Agmip_5June25.csv:/code/inputs/input.csv"
    }
)