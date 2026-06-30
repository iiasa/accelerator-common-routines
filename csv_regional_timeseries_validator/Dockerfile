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

RUN printf '%s\n' \
    '-----BEGIN CERTIFICATE-----' \
    'MIICqzCCAZMCFD7v0fyO16FvxEZxjgzmzv+CySSeMA0GCSqGSIb3DQEBCwUAMBIx' \
    'EDAOBgNVBAMMB2xvY2FsaXAwHhcNMjYwNjMwMDgyMjA1WhcNMzYwNjI3MDgyMjA1' \
    'WjASMRAwDgYDVQQDDAdsb2NhbGlwMIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIB' \
    'CgKCAQEAtRqALZgE6xRaL9P9GPdxuUt9mA2Z2aQTJavIDLfrGkHOk3RScrj6/8VC' \
    '/KxbevPKwHXt3vn3HrQfukefrPXw1ybuwbrFvChTgH29HdxJWEITTClRz8kkIu4r' \
    'zdF1BmCytJapTyfYZyFvMCfKqPZqO+BxGxT6avPYkKJ3XVYjiLBowLgVDlWDOFlz' \
    '+vsK+Tej30xIPOO+6iwAqz3wAlkZeJHKPeP8LhSskzcoIhm1hbxPyWbEZ0DmzjTQ' \
    'KHQYLZoFYW/G9SmsyzU87HzLnNfXNyd7BdxCNF7XBWu3JwV/GkukeYc9sRSx7eRx' \
    '4nC4e77T3tOGN7Gk8WkGacroEOICWQIDAQABMA0GCSqGSIb3DQEBCwUAA4IBAQAu' \
    'eGAMM3xCcdvF+KYUWGa0nssox2soVFF6sDDKpXKwwzK7C+5IGZ3d8qRnJWGNvf5P' \
    's3TUnvQND3FelKT+8AK8oUDh/ZkqBraJRrODegX5SgxMbQgcne14byof7R+VHykK' \
    'CUXA/tw42dSn9yqJ6kpCGG8IOKw8c1S7Y+kSgCMmiCrR0P34jr9vJSJVtsB2FtvP' \
    'qjkHda98Wh+cJ14k7xVPdMSqj6KZbabgjokhqIP6tUc78omJhMtyFCIQYKw1h1bk' \
    'nRWi7vT5sYCniahRXcXKU/+Qn/IhRZVo4PLajqpF5QDLyP24eJisOvLfl3Y/fr4O' \
    'V6Sjwdu5hp+39he8kc9b' \
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

RUN chown -R :0 /agent && chmod -R g+rwX /agent

ENV SSL_CERT_FILE=/agent/ca_bundle.pem