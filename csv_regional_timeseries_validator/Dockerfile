FROM python:3.10.4

WORKDIR /code

RUN apt-get update && \
    apt-get install -y --no-install-recommends ca-certificates && \
    rm -rf /var/lib/apt/lists/*

COPY csv_regional_timeseries_validator/requirements.txt ./requirements.txt

RUN python -m pip install --no-cache-dir -r requirements.txt

COPY csv_regional_timeseries_validator/ .

RUN chown -R :0 /code && chmod -R g+rwX /code

RUN mkdir -p /agent


RUN chmod -R +x /agent/wagt /agent/ssh /agent/xet_python/bin/ || true

RUN chown -R :0 /agent && chmod -R g+rwX /agent

ENV SSL_CERT_FILE=/agent/ca_bundle.pem