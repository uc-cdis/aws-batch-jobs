FROM python:3.9-slim-bullseye

COPY . /bucket-replicate

WORKDIR /bucket-replicate

RUN pip3 install awscli
RUN pip install -r requirements.txt
