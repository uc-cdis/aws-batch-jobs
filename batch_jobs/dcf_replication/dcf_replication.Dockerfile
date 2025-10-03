FROM python:3.9-slim-bullseye

COPY . /dcf_replication

WORKDIR /dcf_replication

RUN pip3 install awscli
RUN pip install -r requirements.txt
