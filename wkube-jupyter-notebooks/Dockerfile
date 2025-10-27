FROM jupyter/datascience-notebook:latest

USER root

RUN apt-get update && apt-get install -y --no-install-recommends git && rm -rf /var/lib/apt/lists/*

RUN pip install --upgrade pip && \
    pip install "jupyter-ai[all]"

RUN jupyter server extension list

ENV JUPYTER_LOG_LEVEL=ERROR

ENV OPENAI_BASE_URL="https://accelerator.iiasa.ac.at/api/v1/openai/v1"

EXPOSE 8888

COPY ./wkube-jupyter-notebooks/entrypoint.sh /usr/local/bin/entrypoint.sh
RUN chmod +x /usr/local/bin/entrypoint.sh

USER $NB_UID
