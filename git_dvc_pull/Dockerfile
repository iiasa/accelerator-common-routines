FROM python:3.10.4

# Install rsync
RUN apt-get update && \
    apt-get install -y rsync findutils && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

COPY ./git_dvc_push/requirements.txt /code/requirements.txt

RUN pip install -r /code/requirements.txt

COPY ./git_dvc_push/ /code

WORKDIR /code

