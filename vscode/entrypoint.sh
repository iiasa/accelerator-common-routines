#!/bin/bash
set -e

# Launch code-server
echo ">>> Ensuring project directory permissions..."
sudo chown coder:coder /home/coder/project || true

echo ">>> Starting code-server (auth disabled)..."
exec code-server --auth none --bind-addr 0.0.0.0:8080 /home/coder/project
