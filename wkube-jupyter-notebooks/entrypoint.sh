#!/bin/bash
set -e

if [ -n "$ACC_JOB_TOKEN" ] && [ -z "$OPENAI_API_KEY" ]; then
  export OPENAI_API_KEY="$ACC_JOB_TOKEN"
fi

echo ">>> Starting JupyterLab with Notebook Intelligence..."

# --- FIX: Changed log_level to INFO to debug extension loading ---
exec start-notebook.sh \
  --ServerApp.ip=0.0.0.0 \
  --ServerApp.token='' \
  --ServerApp.password='' \
  --ServerApp.log_level=ERROR