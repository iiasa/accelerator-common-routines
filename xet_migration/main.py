import argparse
import asyncio
import base64
import json
import os
import secrets
import shutil
import socket
import tempfile
import threading
from datetime import datetime, timezone
from urllib.parse import quote, urlparse

import httpx

REPORT_DIR = "/mnt/wdrv/backfill-reports"
TEMP_ROOT = "/mnt/tmp"
AGENT_PYTHON = "/mnt/tmp/.wkube_agent/xet_python/bin/python3"
HELPER = os.path.join(os.path.dirname(os.path.abspath(__file__)), "xet_upload.py")
CONCURRENCY = 10
RETRY_DELAYS = (2, 8, 32)
PRINT_LOCK = threading.Lock()


def info(message):
    with PRINT_LOCK:
        print(message, flush=True)


def required_env(name):
    value = os.environ.get(name)
    if not value:
        raise SystemExit(f"{name} is not set")
    return value


def utc_now():
    return datetime.now(timezone.utc).isoformat()


class Settings:
    def __init__(self):
        self.gateway = required_env("ACC_JOB_GATEWAY_SERVER").rstrip("/")
        self.recon_url = required_env("RECON_HOST")


def endpoint(url):
    parsed = urlparse(url)
    host = (parsed.hostname or "").lower()
    if parsed.port is not None:
        port = parsed.port
    elif parsed.scheme == "https":
        port = 443
    else:
        port = 80
    return f"{host}:{port}"


def ssl_verify():
    bundle = os.environ.get("SSL_CERT_FILE")
    if bundle and os.path.isfile(bundle):
        return bundle
    return True


class JobAuth:
    def __init__(self):
        self.access_token = None
        self.expires_at = None
        self.lock = asyncio.Lock()

    async def session(self, force=False):
        if self.access_token and not force:
            return self.access_token, self.expires_at
        async with self.lock:
            if self.access_token and not force:
                return self.access_token, self.expires_at
            info("requesting access token from agent")
            self.access_token, self.expires_at = await asyncio.to_thread(
                token_from_agent
            )
            return self.access_token, self.expires_at

    async def token(self, force=False):
        access_token, _expires_at = await self.session(force=force)
        return access_token


def agent_socket_path():
    pod_id = os.environ.get("POD_ID")
    if not pod_id:
        raise SystemExit("POD_ID is not set")
    return f"/mnt/tmp/.wkube_agent/{pod_id}/wagt.sock"


def token_from_agent():
    path = agent_socket_path()
    info(f"connecting to agent socket {path}")
    conn = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    conn.settimeout(30)
    try:
        conn.connect(path)
        conn.sendall(json.dumps({"action": "get-access-token"}).encode())
        conn.shutdown(socket.SHUT_WR)
        chunks = []
        while True:
            chunk = conn.recv(4096)
            if not chunk:
                break
            chunks.append(chunk)
    finally:
        conn.close()
    payload = json.loads(b"".join(chunks))
    access_token = payload.get("access_token")
    expires_at = payload.get("expires_at")
    if payload.get("status") != "success" or not access_token or expires_at is None:
        raise SystemExit(
            f"agent did not return an access token: {payload.get('error')}"
        )
    return access_token, expires_at


def load_filenames(path):
    with open(path) as f:
        filenames = json.load(f)
    if not isinstance(filenames, list):
        raise SystemExit("list must be a JSON array of filenames")
    return filenames


def env_list(*names):
    """Comma-separated selection from the first env var that is set, as wkube_agent passes it."""
    for name in names:
        values = [
            v.strip().rstrip("/")
            for v in os.environ.get(name, "").split(",")
            if v.strip().strip("/")
        ]
        if values:
            return values
    return []


async def node_items(client, settings, folder):
    project_slug = folder.split("/", 1)[0]
    encoded = quote(base64.b64encode(folder.encode()).decode(), safe="")
    url = (
        f"{settings.gateway}/api/v1/aterm-cli/{project_slug}"
        f"/node-items/{encoded}/"
    )
    info(f"node-items {folder}")
    response = await authorized_get(
        client, settings, url, params={"show_hidden": "true"}
    )
    return response.json()


async def selected_from_env(client, settings, catalog, listed_folders):
    """Expand selected_filenames and selected_foldernames into a list of filenames.

    Folders are walked recursively; every node-items response is kept in catalog
    so load_catalog does not list those folders again.
    """
    files = env_list("selected_filenames")
    folders = env_list("selected_foldernames")
    info(f"selected files {len(files)} folders {len(folders)}")
    if not files and not folders:
        raise SystemExit(
            "no list given and selected_filenames / selected_foldernames are empty"
        )
    filenames = list(files)
    for folder in folders:
        pending = [folder]
        while pending:
            path = pending.pop()
            if path in listed_folders:
                continue
            data = await node_items(client, settings, path)
            listed_folders.add(path)
            for item in data["objects"]:
                catalog[item["filename"]] = item
                filenames.append(item["filename"])
            pending.extend(node["absolute_node_name"] for node in data["nodes"])
    return list(dict.fromkeys(filenames))


async def authorized_get(client, settings, url, params=None):
    for attempt in (1, 2):
        token = await settings.auth.token(force=attempt == 2)
        response = await client.get(
            url,
            params=params,
            headers={"x-authorization": token},
        )
        if response.status_code == 401 and attempt == 1:
            info(f"unauthorized, asking agent again {urlparse(url).path}")
            continue
        response.raise_for_status()
        return response


async def download_url(client, settings, filename):
    project_slug = filename.split("/", 1)[0]
    url = f"{settings.gateway}/api/v1/ajob-cli/{project_slug}/get-file-download-url/"
    info(f"get download url {filename}")
    response = await authorized_get(
        client, settings, url, params={"filename": filename}
    )
    return response.json()


async def file_url(client, settings, object_id):
    url = f"{settings.gateway}/api/v1/ajob-cli/file-url/{object_id}/"
    info(f"get file url id={object_id}")
    response = await authorized_get(client, settings, url)
    return response.json()


def parent_node(filename):
    if "/" not in filename:
        raise RuntimeError(f"filename must be project/path: {filename}")
    return filename.rsplit("/", 1)[0]


async def load_catalog(client, settings, filenames, catalog=None, listed_folders=()):
    catalog = {} if catalog is None else catalog
    folders = sorted(
        {parent_node(name) for name in filenames} - set(listed_folders)
    )
    info(f"list {len(folders)} folder(s)")
    for folder in folders:
        for item in (await node_items(client, settings, folder))["objects"]:
            catalog[item["filename"]] = item
    return catalog


def listed(catalog, filename):
    item = catalog.get(filename)
    if item is None:
        raise RuntimeError(f"not listed in node-items: {filename}")
    return item


def split_filename(filename):
    project_slug, remote_path = filename.split("/", 1)
    remote_path = remote_path.lstrip("/")
    if not project_slug or not remote_path:
        raise RuntimeError(f"filename must be project/path: {filename}")
    return project_slug, remote_path


async def register_xet(settings, filename, source_url):
    """Run the agent-python helper, which streams source_url into xet and registers it.

    Returns (http_status, bytes) of the source download as reported by the helper.
    """
    project_slug, remote_path = split_filename(filename)
    access_token, expires_at = await settings.auth.session()
    hf_home = tempfile.mkdtemp(prefix="hf-", dir=TEMP_ROOT)
    env = os.environ.copy()
    env["HF_HOME"] = hf_home
    env["XET_CAS_TOKEN"] = access_token
    env["XET_SOURCE_URL"] = source_url
    cmd = [
        AGENT_PYTHON,
        HELPER,
        project_slug,
        f"{settings.gateway}/api/xet-cas",
        str(expires_at),
        f"{settings.gateway}/api/xet-cas/v1/cas/bulk-register",
        agent_socket_path(),
        remote_path,
    ]
    info(f"stream {filename} from {urlparse(source_url).hostname} into xet")
    try:
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            env=env,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await proc.communicate()
    finally:
        shutil.rmtree(hf_home, ignore_errors=True)
    if proc.returncode != 0:
        detail = (stderr or stdout).decode(errors="replace").strip()
        if len(detail) > 4000:
            detail = detail[-4000:]
        raise RuntimeError(detail or f"xet upload exited {proc.returncode}")
    for line in stdout.decode(errors="replace").splitlines():
        if line.startswith("{"):
            stats = json.loads(line)
            return stats.get("http_status"), stats.get("bytes", 0)
    return None, 0


def new_record(filename, stage):
    return {
        "filename": filename,
        "status": None,
        "stage": stage,
        "url_host": None,
        "http_status": None,
        "attempts": 1,
        "bytes": 0,
        "error": None,
    }


async def upload_one(client, settings, catalog, filename, semaphore):
    record = new_record(filename, "download")
    async with semaphore:
        try:
            item = listed(catalog, filename)
            if item.get("merkle_hash"):
                info(f"skip already xet {filename}")
                record["status"] = "skipped_already_xet"
                return record

            url = await download_url(client, settings, filename)
            record["url_host"] = endpoint(url)

            record["stage"] = "xet"
            record["http_status"], record["bytes"] = await register_xet(
                settings, filename, url
            )
            record["status"] = "migrated"
        except Exception as error:
            record["status"] = "failed"
            record["error"] = f"{type(error).__name__}: {error}"
        return record


async def probe(client, url, filename, attempt):
    info(f"probe {filename} attempt {attempt}")
    async with client.stream("GET", url, headers={"Range": "bytes=0-10"}) as response:
        return response.status_code


async def verify_one(client, settings, catalog, filename, semaphore):
    record = new_record(filename, "probe")
    async with semaphore:
        try:
            item = listed(catalog, filename)
            if not item.get("merkle_hash"):
                info(f"not xet {filename}")
                record["status"] = "not_xet"
                return record

            url = await file_url(client, settings, item["id"])
            record["url_host"] = endpoint(url)

            for attempt in range(1, len(RETRY_DELAYS) + 2):
                record["attempts"] = attempt
                try:
                    record["http_status"] = await probe(client, url, filename, attempt)
                    record["error"] = None
                except httpx.TransportError as error:
                    record["http_status"] = None
                    record["error"] = f"{type(error).__name__}: {error}"

                status = record["http_status"]
                if status is not None and 200 <= status < 300:
                    record["status"] = "ok"
                    return record
                if status is not None and 400 <= status < 500:
                    record["status"] = "http_error"
                    return record
                if attempt <= len(RETRY_DELAYS):
                    delay = RETRY_DELAYS[attempt - 1]
                    info(
                        f"retry {filename} in {delay}s after {status or record['error']}"
                    )
                    await asyncio.sleep(delay)

            record["status"] = "failed"
        except Exception as error:
            record["status"] = "failed"
            record["error"] = f"{type(error).__name__}: {error}"
        return record


def write_report(settings, command, run_id, started_at, records):
    info(f"write report {command}-{run_id}.json")
    os.makedirs(REPORT_DIR, exist_ok=True)
    counts = {}
    for record in records:
        counts[record["status"]] = counts.get(record["status"], 0) + 1
    report = {
        "command": command,
        "run_id": run_id,
        "started_at": started_at,
        "finished_at": utc_now(),
        "recon_host": endpoint(settings.recon_url),
        "total": len(records),
        "counts": counts,
        "files": records,
    }
    path = os.path.join(REPORT_DIR, f"{command}-{run_id}.json")
    with open(path, "w") as f:
        json.dump(report, f, indent=2)
    return path


def log(records):
    for record in sorted(records, key=lambda r: r["filename"]):
        detail = record["error"] or record["http_status"] or ""
        print(f"{record['status']:<20} {record['filename']} {detail}", flush=True)


async def run(command, list_path=None):
    info(f"start {command} list={list_path or 'env selection'}")
    settings = Settings()
    info(f"gateway {settings.gateway} recon {endpoint(settings.recon_url)}")
    settings.auth = JobAuth()
    started_at = utc_now()
    run_id = secrets.token_hex(4)
    semaphore = asyncio.Semaphore(CONCURRENCY)
    timeout = httpx.Timeout(connect=10, read=60, write=60, pool=10)

    async with httpx.AsyncClient(
        timeout=timeout, follow_redirects=True, verify=ssl_verify()
    ) as client:
        catalog, listed_folders = {}, set()
        if list_path:
            info(f"read file list {list_path}")
            filenames = load_filenames(list_path)
        else:
            filenames = await selected_from_env(
                client, settings, catalog, listed_folders
            )
        info(f"files {len(filenames)}")
        await load_catalog(client, settings, filenames, catalog, listed_folders)
        if command == "upload":
            if not os.path.isfile(AGENT_PYTHON):
                raise SystemExit(f"agent python not found: {AGENT_PYTHON}")
            if not os.path.isfile(HELPER):
                raise SystemExit(f"xet helper not found: {HELPER}")
            records = await asyncio.gather(
                *(
                    upload_one(client, settings, catalog, filename, semaphore)
                    for filename in filenames
                )
            )
        else:
            records = await asyncio.gather(
                *(
                    verify_one(client, settings, catalog, filename, semaphore)
                    for filename in filenames
                )
            )

    report_path = await asyncio.to_thread(
        write_report, settings, command, run_id, started_at, records
    )
    log(records)
    print(f"report: {report_path}", flush=True)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("command", choices=["upload", "verify"])
    parser.add_argument(
        "list_path",
        nargs="?",
        help="JSON array of filenames; omit to use selected_filenames / selected_foldernames",
    )
    args = parser.parse_args()
    asyncio.run(run(args.command, args.list_path))


if __name__ == "__main__":
    main()
