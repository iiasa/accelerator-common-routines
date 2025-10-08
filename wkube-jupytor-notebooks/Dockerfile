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



# Enable server extensions (usually already enabled by pip)
RUN jupyter server extension list

# Switch back to the default jovyan user
USER $NB_UID

ENV JUPYTER_TOKEN=""
ENV JUPYTER_PASSWORD=""
ENV JUPYTER_LOG_LEVEL=ERROR

# =====================================================
# Set environment variables (optional for Groq)
# =====================================================
# These can also be set at runtime via -e flags.
# ENV OPENAI_API_KEY="gsk_abc123YOURKEY"
# ENV OPENAI_API_BASE_URL="https://api.groq.com/openai/v1"
# ENV JUPYTER_AI_MODEL=openai/gpt-oss-120b


# Loading ai magic extension
# %load_ext jupyter_ai_magics
# Usage example
# %%ai openai-chat-custom:openai/gpt-oss-120b -f code
# Generate me a histogram of following dataset

# =====================================================
# Expose JupyterLab
# =====================================================
EXPOSE 8888

COPY ./wkube-jupytor-notebooks/README.ai.agent.md /home/jovyan/work/

# Default CMD is inherited from jupyter/datascience-notebook
CMD ["start-notebook.sh", "--NotebookApp.ip=0.0.0.0"]
