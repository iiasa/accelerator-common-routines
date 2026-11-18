FROM --platform=linux/amd64 python:3.11-slim

COPY ./xet_migration/requirements.txt /code/requirements.txt

RUN pip install --no-cache-dir -r /code/requirements.txt

COPY ./xet_migration/ /code

RUN chown -R 65534:65534 /code

WORKDIR /code

USER 65534:65534
