FROM python:3.9-slim-bullseye

COPY . /bucket-manifest

WORKDIR /bucket-manifest

RUN pip3 install awscli
RUN pip install -r requirements.txt
