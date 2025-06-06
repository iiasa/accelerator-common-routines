FROM --platform=linux/amd64 python:3.9

RUN apt-get update && \
    apt-get install -y \
    gdal-bin \
    libgdal-dev \
    python3-gdal \
    build-essential \
    python3-dev \
    git 

ENV CPLUS_INCLUDE_PATH=/usr/include/gdal
ENV C_INCLUDE_PATH=/usr/include/gdal

COPY ./tif_to_cog_converter/requirements.txt /code/requirements.txt

RUN pip install --upgrade pip setuptools wheel

RUN pip install -r /code/requirements.txt

COPY ./tif_to_cog_converter/ /code

WORKDIR /code