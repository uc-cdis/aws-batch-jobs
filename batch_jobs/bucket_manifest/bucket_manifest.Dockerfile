FROM python:3.6

RUN pip install -r requirements.txt

COPY . /bucket-manifest

WORKDIR /bucket-manifest
