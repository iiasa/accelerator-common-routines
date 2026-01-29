#!/bin/bash
set -e

if [ -n "$ACC_JOB_TOKEN" ] && [ -z "$OPENAI_API_KEY" ]; then
  export OPENAI_API_KEY="$ACC_JOB_TOKEN"
fi

echo ">>> Starting JupyterLab with Notebook Intelligence..."
exec start-lab.sh \
  --LabApp.ip=0.0.0.0 \
  --LabApp.token='' \
  --LabApp.password='' \
  --ServerApp.log_level=ERROR
