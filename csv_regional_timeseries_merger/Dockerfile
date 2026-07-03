FROM python:3.10.4

COPY ./csv_regional_timeseries_merger/requirements.txt /code/requirements.txt

ARG CACHE_BUST=1

RUN echo "Cache bust: ${CACHE_BUST}" && \
    pip install -r /code/requirements.txt

COPY ./csv_regional_timeseries_merger/ /code

RUN chown -R 65534:65534 /code && chmod -R g+rwX /code

WORKDIR /code

