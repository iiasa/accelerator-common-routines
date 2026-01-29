#!/bin/bash
set -e

# 1. Map Accelerator Token to OpenAI Key
if [ -n "$ACC_JOB_TOKEN" ] && [ -z "$OPENAI_API_KEY" ]; then
  export OPENAI_API_KEY="$ACC_JOB_TOKEN"
fi

echo ">>> Configuring Notebook Intelligence..."

# 2. Inject Configuration with Python
# We explicitly construct the complex JSON object to ensure the key lands in the 'properties' list
python3 -c "
import json
import os

key = os.environ.get('OPENAI_API_KEY', '')
base_url = os.environ.get('OPENAI_BASE_URL', 'https://accelerator.iiasa.ac.at/api/v1/openai/v1')
config_path = '/home/jovyan/.jupyter/nbi/config.json'

# Ensure directory exists
os.makedirs(os.path.dirname(config_path), exist_ok=True)

# Define the exact config structure required by the extension
config = {
    'llm_provider': 'openai-compatible',
    'default_chat_mode': 'ask',
    'chat_model': {
        'provider': 'openai-compatible',
        'model': 'openai-compatible-chat-model',
        'properties': [
            { 'id': 'api_key', 'name': 'API key', 'value': key, 'optional': False },
            { 'id': 'model_id', 'name': 'Model', 'value': 'gpt-4o', 'optional': False },
            { 'id': 'base_url', 'name': 'Base URL', 'value': base_url, 'optional': True }
        ]
    },
    'inline_completion_model': {
        'provider': 'openai-compatible',
        'model': 'openai-compatible-inline-completion-model',
        'properties': [
            { 'id': 'api_key', 'name': 'API key', 'value': key, 'optional': False },
            { 'id': 'model_id', 'name': 'Model', 'value': 'gpt-4o', 'optional': False },
            { 'id': 'base_url', 'name': 'Base URL', 'value': base_url, 'optional': True }
        ]
    },
    'inline_completion_debouncer_delay': 200
}

# Write to file
with open(config_path, 'w') as f:
    json.dump(config, f, indent=2)

print(f'>>> Notebook Intelligence configured for user {os.getuid()}')
"

echo ">>> Starting JupyterLab..."

# 3. Launch JupyterLab
exec jupyter lab \
  --ServerApp.ip=0.0.0.0 \
  --ServerApp.token='' \
  --ServerApp.password='' \
  --ServerApp.allow_origin='*' \
  --ServerApp.log_level='INFO'