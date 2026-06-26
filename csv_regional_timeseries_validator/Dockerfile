FROM python:3.10.4

WORKDIR /code

RUN apt-get update && \
    apt-get install -y --no-install-recommends ca-certificates && \
    rm -rf /var/lib/apt/lists/*

COPY csv_regional_timeseries_validator/requirements.txt ./requirements.txt

RUN python -m pip install --no-cache-dir -r requirements.txt

COPY csv_regional_timeseries_validator/ .

RUN mkdir -p /agent

RUN printf '%s\n' \
    '-----BEGIN CERTIFICATE-----' \
    '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' \
    'YcyrRxa6H1DP6+7L4gDNkyI=' \
    '-----END CERTIFICATE-----' \
    > /agent/backend.crt

RUN printf '%s\n' \
    '-----BEGIN CERTIFICATE-----' \
    'MIIEzTCCA7WgAwIBAgIUe88S98iszU8TxVQL7fQhi48wKXgwDQYJKoZIhvcNAQEL' \
    'BQAwfjELMAkGA1UEBhMCQVUxDTALBgNVBAgMBFdpZW4xDTALBgNVBAcMBFdpZW4x' \
    'EDAOBgNVBAoMB0JhYmJsZXIxCzAJBgNVBAsMAklUMRAwDgYDVQQDDAdsb2NhbGlw' \
    'MSAwHgYJKoZIhvcNAQkBFhF3cnVmZXNoQGdtYWlsLmNvbTAeFw0yNjA1MTkyMDEz' \
    'MjRaFw0zNjA1MTYyMDEzMjRaMH4xCzAJBgNVBAYTAkFVMQ0wCwYDVQQIDARXaWVu' \
    'MQ0wCwYDVQQHDARXaWVuMRAwDgYDVQQKDAdCYWJibGVyMQswCQYDVQQLDAJJVDEQ' \
    'MA4GA1UEAwwHbG9jYWxpcDEgMB4GCSqGSIb3DQEJARYRd3J1ZmVzaEBnbWFpbC5j' \
    'b20wggEiMA0GCSqGSIb3DQEBAQUAA4IBDwAwggEKAoIBAQCgzrLzoqIy2nIV5C5n' \
    'RJzxETJQGEqtAJIoQiHVgxTMJ83RT2FhKO15ClIApGt5973qvxambKquAkf5e9qj' \
    'f7Iit64UmEQzSPCLcTwLJukJD6jpNcHbSTtwySI9Ox+1kYzeuYitbWmg54DiIwIJ' \
    '3WApuI8/5yIwc9OQmUYX8aFxTIJG3DxmRosecwW7ha/vquPpRvwKbxh+Thr5irds' \
    'ELrNHRM8bB6O9HQD9KZADxPoH2ERaYjWBICjUr1UQqNzFsY8Cqk1uXSWIJ6cEhm8' \
    'MvodZV/CKs3sJwsUaGMsLLUS4TZ4ubVEnfEKKm/pVEwVcAqtiDed/NcPKXuEZMaj' \
    'yZVBAgMBAAGjggFBMIIBPTAdBgNVHQ4EFgQUaB+ScvBCTu8bDoRuF6J4ow1QaHow' \
    'HwYDVR0jBBgwFoAUaB+ScvBCTu8bDoRuF6J4ow1QaHowCQYDVR0TBAIwADALBgNV' \
    'HQ8EBAMCBaAwHQYDVR0lBBYwFAYIKwYBBQUHAwEGCCsGAQUFBwMCMIHDBgNVHREE' \
    'gbswgbiCCWxvY2FsaG9zdIIHbG9jYWxpcIIFbWluaW+CCHJlZ2lzdHJ5ggZ3ZWJf' \
    'YmWCHmx1bWV4cC1sb2NhbC5hY2Ntcy5paWFzYS5hYy5hdIIcbHVtZXhwLWxvYy5h' \
    'Y2Ntcy5paWFzYS5hYy5hdIIPbHBkLmlpYXNhLmFjLmF0gg9hZG1pbi5sb2NhbGhv' \
    'c3SCEXRlbmFudDEubG9jYWxob3N0ggpsb2NhbHJlY29uhwR/AAABhwQKAACNMA0G' \
    'CSqGSIb3DQEBCwUAA4IBAQCCc9RQ/RFKO/mHhJNcPmEwBJVOl1l7rH08kkMvJ+8d' \
    '6BTVv63EV+94R/scuOx78tUcbEweUmApIxpc/pRSGjY7vN9WILGEmTJtnOS1gPpO' \
    '+3ieZYLtJ12osV34OEoS9o8XTHLNALtdCY+YOkgjQY5dCbQSySHrEKXJeo331QWu' \
    'e3jvI/PjBtmuwnK3Bhwxb9WwVdIf5DBDx8V6BkHytykLZVr4UHIRB7y7fkAqcHTu' \
    'u4Osx4cDLsp7QXTyq9inzSBVGlSDo/DfocO0Om8pvRD2E/jjz5L6dRdHf6/+tpZG' \
    'NUYknrHgkoZ0ibwV2J9H5hBmfeWvtzEJQvlEghxpmbo1' \
    '-----END CERTIFICATE-----' \
    > /agent/minio.crt

RUN cat /etc/ssl/certs/ca-certificates.crt \
    /agent/backend.crt \
    /agent/minio.crt \
    > /agent/ca_bundle.pem && \
    cat /agent/backend.crt /agent/minio.crt \
    >> /etc/ssl/certs/ca-certificates.crt

RUN chmod -R +x /agent/wagt /agent/ssh /agent/xet_python/bin/ || true

ENV SSL_CERT_FILE=/agent/ca_bundle.pem