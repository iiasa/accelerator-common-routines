#!/bin/bash
# entrypoint.sh

# Alias OPENAI_API_KEY to ACC_JOB_TOKEN if it's set
if [ -n "$ACC_JOB_TOKEN" ] && [ -z "$OPENAI_API_KEY" ]; then
  export OPENAI_API_KEY="$ACC_JOB_TOKEN"
fi

# Then exec the main Jupyter command
exec "$@"