FROM python:3.10.4

COPY ./csv_regional_timeseries_merger/requirements.txt /code/requirements.txt

RUN pip install -r /code/requirements.txt

COPY ./csv_regional_timeseries_merger/ /code

WORKDIR /code

