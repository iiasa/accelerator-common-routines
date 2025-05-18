FROM python:3.10.4

COPY ./requirements.txt /app/requirements.txt

RUN pip install -r /app/requirements.txt

COPY ./ /app

WORKDIR /app

