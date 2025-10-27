#!/bin/bash
set -e

# =====================================================
# Alias OPENAI_API_KEY to ACC_JOB_TOKEN (if set)
# =====================================================
if [ -n "$ACC_JOB_TOKEN" ] && [ -z "$OPENAI_API_KEY" ]; then
  export OPENAI_API_KEY="$ACC_JOB_TOKEN"
fi

# =====================================================
# Create Jupyter AI config file at runtime
# =====================================================
CONFIG_DIR="/home/jovyan/.local/share/jupyter/jupyter_ai"
CONFIG_FILE="$CONFIG_DIR/config.json"

mkdir -p "$CONFIG_DIR"

cat > "$CONFIG_FILE" <<EOF
{
    "model_provider_id": "openai-chat-custom:gpt-4o-mini",
    "embeddings_provider_id": "openai:text-embedding-3-large",
    "send_with_shift_enter": false,
    "fields": {
        "openai-chat-custom:gpt-4o-mini": {
            "openai_api_base": "https://accelerator.iiasa.ac.at/api/v1/openai/v1"
        }
    },
    "api_keys": {
        "OPENAI_API_KEY": "${OPENAI_API_KEY}"
    },
    "completions_model_provider_id": null,
    "completions_fields": {},
    "embeddings_fields": {}
}
EOF

# =====================================================
# Create IPython startup script for Jupyter AI magics
# =====================================================
IPYTHON_STARTUP_DIR="/home/jovyan/.ipython/profile_default/startup"
mkdir -p "$IPYTHON_STARTUP_DIR"

cat > "$IPYTHON_STARTUP_DIR/00-load-jupyter-ai.py" <<'EOF'
ip = get_ipython()
if ip is not None:
    ip.run_line_magic('load_ext', 'jupyter_ai_magics')
    ip.run_line_magic('config', "AiMagics.initial_language_model = 'openai-chat:gpt-4o-mini'")
EOF

# =====================================================
# Adjust ownership to the jovyan user
# =====================================================
chown -R "$NB_UID:$NB_GID" /home/jovyan/.local /home/jovyan/.ipython || true

echo "export OPENAI_API_KEY=${OPENAI_API_KEY}" >> /home/jovyan/.bashrc

# =====================================================
# Start Jupyter Notebook
# =====================================================
echo ">>> Starting Jupyter Notebook..."
exec start-notebook.sh \
  --NotebookApp.ip=0.0.0.0 \
  --NotebookApp.token='' \
  --NotebookApp.password='' \
  --ServerApp.log_level=ERROR
