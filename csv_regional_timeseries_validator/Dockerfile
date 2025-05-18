FROM python:3.10.4

COPY ./csv_regional_timeseries_validator/requirements.txt /app/requirements.txt

RUN pip install -r /app/requirements.txt

COPY ./csv_regional_timeseries_validator/ /app

WORKDIR /app

