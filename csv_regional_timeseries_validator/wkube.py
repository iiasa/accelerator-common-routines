from accli import WKubeTask

regional_validator = WKubeTask(
    name="Buffered Python Test",
    job_folder='./',
    # repo_url="https://github.com/iiasa/accelerator-common-routines.git",
    # repo_branch="master",
    docker_filename="Dockerfile.wkube",
    command="python main.py",
    required_cores=1,
    required_ram=1024*1024*1024,
    required_storage_local=1024*1024*1024,
    required_storage_workflow=1024,
    timeout=3600,
    conf={
        "dataset_template_id": 4,
        "VERIFY_ONLY":'True',
        "input_mappings": "acc://forestnav/pathway/pathway_sample.csv:/code/inputs/input.csv"
    }
)