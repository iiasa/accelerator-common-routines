FROM python:3.10.4

COPY ./csv_regional_timeseries_merger/requirements.txt /app/requirements.txt

RUN pip install -r /app/requirements.txt

COPY ./csv_regional_timeseries_merger/ /app

WORKDIR /app

