# Xet migration

Moves cloud-storage files that are not yet xet-backed into xet, and verifies
that xet-backed files are servable.

## Modes

- `upload` — for each file that has no merkle hash, get a download URL, stream
  the bytes into xet with the agent python helper, then register the file.
- `verify` — for each file that has a merkle hash, get a file URL and probe the
  first bytes with a range request. Failed probes are retried three times.

## Input

The file list comes from one of two places:

- the `selected_filenames` and `selected_foldernames` environment variables,
  which the platform sets from the files and folders you select. Folders are
  walked recursively.
- a JSON array of filenames given as the second argument, e.g.
  `python main.py upload /mnt/wdrv/my-list.json`.

Each filename is `project-slug/path/to/file`.

## Output

A report is written to `/mnt/wdrv/backfill-reports/<mode>-<run-id>.json` with
one record per file: status, stage, HTTP status, bytes and error. The job log
prints the same records and the report path.

## Environment

- `ACC_JOB_GATEWAY_SERVER`, `POD_ID` — set by the platform.
- `RECON_HOST` — the reconstruction host the report records.

The upload mode needs the agent python at
`/mnt/tmp/.wkube_agent/xet_python/bin/python3`, so it only runs on WKube.
