import subprocess
import os
import sys
from urllib.parse import urlparse, urlunparse

bucket = os.getenv["DVC_S3_BUCKET"]
prefix = os.getenv("DVC_S3_PREFIX", "")
access_key = os.getenv["AWS_ACCESS_KEY_ID"]
secret_key = os.getenv["AWS_SECRET_ACCESS_KEY"]
endpoint = os.getenv["DVC_S3_ENDPOINT_URL"]

repo_data_folder = os.getenv["REPO_DATA_FOLDER"]
commit_message = os.getenv["COMMIT_MESSAGE"]

repo_url = os.getenv["GIT_REPO_URL_HTTP"]
branch_name = os.getenv["BRANCH_NAME"]
pat_token = os.getenv("GIT_PAT")

def run_command(command, cwd=None):
    """Run a shell command and print its output."""
    print(f"Running: {' '.join(command)}")
    result = subprocess.run(command, cwd=cwd, capture_output=True, text=True)
    if result.returncode != 0:
        print("Error:", result.stderr)
        sys.exit(result.returncode)
    print(result.stdout)

def inject_pat_into_url(repo_url, pat):
    """Inject the PAT into the HTTPS Git URL."""
    parsed = urlparse(repo_url)
    if parsed.scheme != "https":
        raise ValueError("Warning: GIT_PAT is set but repo URL is not HTTPS. Skipping token injection.")
    netloc = f"{pat}@{parsed.netloc}"
    return urlunparse((parsed.scheme, netloc, parsed.path, '', '', ''))

def configure_dvc_s3_remote(repo_path):

    remote_url = f"s3://{bucket}/{prefix}".rstrip("/")

    # Add and configure DVC remote locally
    run_command(["dvc", "remote", "add", "--local", "-d", "storage", remote_url], cwd=repo_path)
    run_command(["dvc", "remote", "modify", "--local", "storage", "access_key_id", access_key], cwd=repo_path)
    run_command(["dvc", "remote", "modify", "--local", "storage", "secret_access_key", secret_key], cwd=repo_path)
    run_command(["dvc", "remote", "modify", "--local", "storage", "endpointurl", endpoint], cwd=repo_path)

def clone_git_repo(repo_url, destination="."):
    """Clone the Git repository."""
    run_command(["git", "clone","-b", branch_name, repo_url], cwd=destination)

def pull_dvc_data(repo_path):
    """Pull data with DVC in the cloned repo."""
    run_command(["dvc", "pull"], cwd=repo_path)

def add_new_files(repo_path):
    

    run_command(['rsync', '-avh', '/app/workdir/newfiles/', f"{repo_path}/{repo_data_folder}"], cwd=repo_path)
    for root, dirs, files in os.walk(f"{repo_path}/{repo_data_folder}"):
        for file in files:
            file_path = os.path.join(root, file)
            print(f"Adding {file_path} to DVC...")
            subprocess.run(["dvc", "add", file_path])
    
    run_command(["dvc", "push"], cwd=repo_path)

    # Commit and push to Git
    run_command(["git", "add", "."], cwd=repo_path)
    run_command(["git", "commit", "-m", commit_message], cwd=repo_path)
    run_command(["git", "push", "origin", "HEAD"], cwd=repo_path)
    

def main():

    repo_url = inject_pat_into_url(repo_url, pat_token)

    # Main operations
    clone_git_repo(repo_url, destination='/app/workdir/repo')
    configure_dvc_s3_remote('/app/workdir/repo')
    # pull_dvc_data(repo_name)
    add_new_files('/app/workdir/repo')

if __name__ == "__main__":
    main()
