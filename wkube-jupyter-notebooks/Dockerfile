FROM jupyter/datascience-notebook:latest

USER root

# Install the extension
RUN pip install --upgrade pip \
    && pip install --upgrade "jupyterlab>=4.2.0" notebook-intelligence

# --- FIX 1: Pre-configure Notebook Intelligence to use OpenAI ---
# We create the config file manually so you don't have to click the UI
# We use /home/jovyan/.jupyter because that is where the app looks by default
RUN mkdir -p /home/jovyan/.jupyter/nbi && \
    echo '{ \
    "llm_provider": "openai", \
    "openai_model": "gpt-4o", \
    "openai_base_url": "https://accelerator.iiasa.ac.at/api/v1/openai/v1" \
    }' > /home/jovyan/.jupyter/nbi/config.json

# --- FIX 2: Correct Permissions ---
# Ensure the jovyan user owns the new config file
RUN chown -R $NB_UID:$NB_GID /home/jovyan/.jupyter

ENV JUPYTER_ENABLE_LAB=yes
ENV OPENAI_BASE_URL="https://accelerator.iiasa.ac.at/api/v1/openai/v1"

COPY ./wkube-jupyter-notebooks/entrypoint.sh /usr/local/bin/entrypoint.sh
RUN chmod +x /usr/local/bin/entrypoint.sh

USER $NB_UID
EXPOSE 8888