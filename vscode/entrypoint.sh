#!/bin/bash
set -e

# Launch code-server
echo ">>> Ensuring project directory permissions..."
sudo chown coder:coder /home/coder/project || true

# Folder to open in code-server; override with WORKSPACE_DIR (e.g. /mnt/wdrv)
WORKSPACE_DIR="${WORKSPACE_DIR:-/mnt/wdrv}"

echo ">>> Starting code-server (auth disabled) in $WORKSPACE_DIR..."
exec code-server --auth none --bind-addr 0.0.0.0:8080 "$WORKSPACE_DIR"
