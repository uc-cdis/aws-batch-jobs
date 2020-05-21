FROM python:3.6

COPY . /bucket-manifest

WORKDIR /bucket-manifest

RUN pip install -r requirements.txt
