import json
import os
import socket
import ssl
import sys
import urllib.error
import urllib.request


def fail(message):
    print(message, file=sys.stderr)
    raise SystemExit(1)


def wrapped_token(project_slug, access_token):
    return f"xet_session_prj_{project_slug}_{access_token}"


def refresh(socket_path, project_slug):
    conn = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    conn.settimeout(30)
    try:
        conn.connect(socket_path)
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
        fail(f"agent did not return an access token: {payload.get('error')}")
    return wrapped_token(project_slug, access_token), int(expires_at)


def register(register_url, token, project_slug, item):
    req = urllib.request.Request(
        register_url,
        data=json.dumps({"items": [item]}).encode(),
        headers={
            "Content-Type": "application/json",
            "X-Project-Slug": project_slug,
            "Authorization": f"Bearer {token}",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(req) as resp:
            return resp.status, resp.read().decode()
    except urllib.error.HTTPError as err:
        return err.code, err.read().decode()


CHUNK = 1 << 20


def ssl_context():
    bundle = os.environ.get("SSL_CERT_FILE")
    if bundle and os.path.isfile(bundle):
        return ssl.create_default_context(cafile=bundle)
    return ssl.create_default_context()


def upload(endpoint, token, expires_at, source_url, name):
    """Stream source_url straight into xet; nothing is written to disk."""
    import hf_xet

    refresh_url = endpoint.split("/api/")[0] + "/api/v1/oauth/device/cas-token/"
    session = hf_xet.XetSession()
    size = 0
    with urllib.request.urlopen(
        source_url, timeout=60, context=ssl_context()
    ) as response:
        http_status = response.status
        with session.new_upload_commit(
            endpoint=endpoint,
            token=token,
            token_expiry_unix_secs=int(expires_at),
            token_refresh_url=refresh_url,
            token_refresh_headers={"Authorization": f"Bearer {token}"},
        ) as commit:
            stream = commit.start_upload_stream(name=name)
            try:
                while True:
                    chunk = response.read(CHUNK)
                    if not chunk:
                        break
                    stream.write(chunk)
                    size += len(chunk)
                result = stream.finish()
            except BaseException:
                stream.abort()
                raise
    print(json.dumps({"http_status": http_status, "bytes": size}), flush=True)
    return result, size


def main():
    if len(sys.argv) != 7:
        fail(
            "usage: xet_upload.py slug endpoint expires register_url socket remote"
        )
    (
        project_slug,
        endpoint,
        expires_at,
        register_url,
        socket_path,
        remote_path,
    ) = sys.argv[1:7]
    access_token = os.environ.get("XET_CAS_TOKEN")
    if not access_token:
        fail("XET_CAS_TOKEN is not set")
    # Passed through env, not argv, so the signed URL doesn't show up in ps.
    source_url = os.environ.get("XET_SOURCE_URL")
    if not source_url:
        fail("XET_SOURCE_URL is not set")

    token = wrapped_token(project_slug, access_token)
    result, size = upload(endpoint, token, expires_at, source_url, remote_path)
    item = {
        "filename": f"{project_slug}/{remote_path}",
        "merkle_hash": result.xet_info.hash,
        "file_size": size,
        "content_type": "application/octet-stream",
    }
    status, body = register(register_url, token, project_slug, item)
    if status == 401:
        token, _expires_at = refresh(socket_path, project_slug)
        status, body = register(register_url, token, project_slug, item)
    if not 200 <= status < 300:
        fail(f"REGISTER_FAILED: {status} - {body}")
    print("REGISTER_SUCCESS", flush=True)


if __name__ == "__main__":
    main()
