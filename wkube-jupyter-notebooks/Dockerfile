FROM jupyter/datascience-notebook:latest

USER root
RUN apt-get update && apt-get install -y --no-install-recommends git \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --upgrade pip \
    && pip install notebook-intelligence

# --- FIX: Explicitly enable the server extension for the system ---
# This ensures the 'jovyan' user sees the extension installed by root
RUN jupyter server extension enable notebook_intelligence --sys-prefix

# (Optional) Pre-configure settings so it defaults to OpenAI instead of Copilot
# We will create an overrides.json file in the next step
COPY ./wkube-jupyter-notebooks/overrides.json /opt/conda/share/jupyter/lab/settings/overrides.json

ENV OPENAI_BASE_URL="https://accelerator.iiasa.ac.at/api/v1/openai/v1"
# REMOVE THIS for now so you can see if the extension crashes at startup
# ENV JUPYTER_LOG_LEVEL=ERROR 

COPY ./wkube-jupyter-notebooks/entrypoint.sh /usr/local/bin/entrypoint.sh
RUN chmod +x /usr/local/bin/entrypoint.sh

USER $NB_UID
EXPOSE 8888