#!/bin/bash
set -e

# 1. Handle API Key mapping
if [ -n "$ACC_JOB_TOKEN" ] && [ -z "$OPENAI_API_KEY" ]; then
  export OPENAI_API_KEY="$ACC_JOB_TOKEN"
fi

echo ">>> Configuring Notebook Intelligence..."

# 2. Inject the API key into the config file safely using Python (avoiding sed syntax mess)
# This updates the config.json we created in the Dockerfile with the actual key from env
python3 -c "import json, os; \
p = '/home/jovyan/.jupyter/nbi/config.json'; \
d = json.load(open(p)) if os.path.exists(p) else {}; \
d['openai_api_key'] = os.environ.get('OPENAI_API_KEY', ''); \
json.dump(d, open(p, 'w'))"

echo ">>> Starting JupyterLab..."

# 3. FORCE JupyterLab execution (fixes the missing icon issue)
# 'start-notebook.sh' launches the classic tree view by default in some versions
# unless JUPYTER_ENABLE_LAB is set, but calling 'jupyter lab' directly is safer.
exec jupyter lab \
  --ServerApp.ip=0.0.0.0 \
  --ServerApp.token='' \
  --ServerApp.password='' \
  --ServerApp.allow_origin='*' \
  --ServerApp.log_level='INFO'