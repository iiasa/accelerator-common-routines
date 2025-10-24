# =====================================================
# Start from full Jupyter scientific base
# =====================================================
FROM jupyter/datascience-notebook:latest

# Switch to root to install global packages
USER root

# Optional: update system packages
RUN apt-get update && apt-get install -y --no-install-recommends git && rm -rf /var/lib/apt/lists/*

# =====================================================
# Install Jupyter AI and extensions
# =====================================================
RUN pip install --upgrade pip && \
    pip install "jupyter-ai[all]"

# =====================================================
# Auto-load AI magic and set default model
# =====================================================
RUN mkdir -p /home/jovyan/.ipython/profile_default/startup
RUN echo "\
%load_ext jupyter_ai_magics\n\
%config AiMagics.initial_language_model = 'openai/gpt-4o-mini'\n\
" > /home/jovyan/.ipython/profile_default/startup/00-load-jupyter-ai.py

# Enable server extensions (usually already enabled by pip)
RUN jupyter server extension list

# These files live under ~/.jupyter/lab/user-settings/@jupyter-ai/
RUN mkdir -p /home/jovyan/.jupyter/lab/user-settings/@jupyter-ai/core-extension && \
    echo '{ \
  "defaultProvider": "openai", \
  "defaultModel": "openai/gpt-4o-mini", \
  "defaultSystemPrompt": "You are a helpful data science assistant in JupyterLab." \
}' > /home/jovyan/.jupyter/lab/user-settings/@jupyter-ai/core-extension/plugin.jupyterlab-settings

# Open chat sidebar by default
RUN mkdir -p /home/jovyan/.jupyter/lab/user-settings/@jupyter-ai/chat-extension && \
    echo '{"sidebarVisible": true}' > /home/jovyan/.jupyter/lab/user-settings/@jupyter-ai/chat-extension/plugin.jupyterlab-settings


# Switch back to the default jovyan user


ENV JUPYTER_LOG_LEVEL=ERROR

# =====================================================
# Set environment variables (optional for Groq)
# =====================================================
# These can also be set at runtime via -e flags.
# ENV OPENAI_API_KEY="gsk_abc123YOURKEY"
ENV OPENAI_API_BASE_URL="https://accelerator.iiasa.ac.at/api/v1/openai/v1"


# =====================================================
# Expose JupyterLab
# =====================================================
EXPOSE 8888

# COPY ./wkube-jupyter-notebooks/README.ai.agent.md /home/jovyan/work/

COPY ./wkube-jupyter-notebooks/entrypoint.sh /usr/local/bin/entrypoint.sh
RUN chmod +x /usr/local/bin/entrypoint.sh

USER $NB_UID

ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]

# Default CMD is inherited from jupyter/datascience-notebook
CMD ["start-notebook.sh", "--NotebookApp.ip=0.0.0.0"]
