FROM python:3.6

COPY . /bucket-replicate

WORKDIR /bucket-replicate

RUN pip3 install awscli
RUN pip install -r requirements.txt
