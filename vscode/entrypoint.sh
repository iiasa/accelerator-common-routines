#!/bin/bash
set -e

# 1. Map Accelerator Token to Roo Code Configuration
if [ -n "$ACC_JOB_TOKEN" ]; then
  echo ">>> Configuring Roo Code with Accelerator Token..."
  
  # VS Code and Roo Code settings paths
  USER_DATA_DIR="/home/coder/.local/share/code-server"
  VSCODE_SETTINGS="$USER_DATA_DIR/User/settings.json"
  GLOBAL_STORAGE="$USER_DATA_DIR/User/globalStorage/storage.json"
  STATE_DB="$USER_DATA_DIR/User/globalStorage/state.vscdb"
  AUTO_IMPORT_PATH="/home/coder/.roocode_settings.json"
  
  # Ensure directories exist with correct owner permissions
  # In code-server, /home/coder is owned by coder
  mkdir -p "$(dirname "$VSCODE_SETTINGS")" || true
  mkdir -p "$(dirname "$GLOBAL_STORAGE")" || true

  # 1. Update JSON settings via Python
  python3 -c "
import json
import os
import uuid

token = os.environ.get('ACC_JOB_TOKEN', '')
endpoint = 'https://accelerator.iiasa.ac.at/api/v1/openai/v1'
model = 'gpt-4o'
ext_id = 'RooVeterinaryInc.Roo-Code'
internal_id = 'rooveterinaryinc.roo-cline'

def update_json(path, data, key=None):
    existing = {}
    if os.path.exists(path):
        try:
            with open(path, 'r') as f:
                existing = json.load(f)
        except: pass
    
    if key:
        if key not in existing:
            existing[key] = {}
        if isinstance(existing[key], dict):
            existing[key].update(data)
        else:
            existing[key] = data
    else:
        existing.update(data)
        
    with open(path, 'w') as f:
        json.dump(existing, f, indent=2)

# Create the profile configuration
# Roo Code uses a specific structure for profiles in 'roo_cline_config_api_config'
profile_id = 'accelerator-profile-id' # Static ID for consistency
api_config = {
    'currentApiConfigName': 'Accelerator',
    'apiConfigs': {
        'Accelerator': {
            'id': profile_id,
            'apiProvider': 'openai',
            'openAiBaseUrl': endpoint,
            'openAiModelId': model,
            'openAiApiKey': token
        }
    },
    'modeApiConfigs': {
        'code': profile_id,
        'architect': profile_id,
        'ask': profile_id
    },
    'migrations': {
        'rateLimitSecondsMigrated': True,
        'openAiHeadersMigrated': True,
        'consecutiveMistakeLimitMigrated': True,
        'todoListEnabledMigrated': True,
        'claudeCodeLegacySettingsMigrated': True
    }
}

# Update settings.json
vscode_settings = {
    'roo-cline.autoImportSettingsPath': '$AUTO_IMPORT_PATH',
    'roo-cline.allowedCommands': ['*'],
    'roo-cline.autoApprovalEnabled': True
}
update_json('$VSCODE_SETTINGS', vscode_settings)

# Update globalStorage/storage.json for onboarding bypass
storage_data = {
    'lastShownAnnouncementId': 'jul-09-2025-3-23-0',
    'hasOpenedModeSelector': True,
    'currentApiConfigName': 'Accelerator'
}
update_json('$GLOBAL_STORAGE', storage_data, key=internal_id)

# Create the auto-import file (as a backup/documented way)
with open('$AUTO_IMPORT_PATH', 'w') as f:
    json.dump({'providerProfiles': api_config, 'globalSettings': storage_data}, f, indent=2)

# Export api_config to a temp file for sqlite3 injection
with open('/tmp/roo_api_config.json', 'w') as f:
    json.dump(api_config, f)

print(f'>>> Roo Code state and settings prepared for user {os.getuid()}')
"

  # 2. Direct SQLite injection for secrets and profiles
  # Roo Code stores the entire profile structure in a secret key
  if [ -f "$STATE_DB" ] && command -v sqlite3 >/dev/null 2>&1; then
    echo ">>> Injecting Roo Code profiles and secrets into state.vscdb..."
    API_CONFIG_JSON=$(cat /tmp/roo_api_config.json)
    sqlite3 "$STATE_DB" <<EOF
INSERT OR REPLACE INTO ItemTable (key, value) VALUES ('roo_cline_config_api_config', '$API_CONFIG_JSON');
EOF
    rm /tmp/roo_api_config.json
  fi
else
  echo ">>> Warning: ACC_JOB_TOKEN not set. Roo Code will not be pre-configured."
fi

# 2. Launch code-server
echo ">>> Starting code-server (auth disabled)..."
exec code-server --auth none --bind-addr 0.0.0.0:8080 /home/coder/project
