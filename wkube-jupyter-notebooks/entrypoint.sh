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

# Adjust ownership to the jovyan user (non-root)
chown -R $NB_UID:$NB_GID "$CONFIG_DIR"

# =====================================================
# Continue with the main Jupyter command
# =====================================================
exec "$@"
