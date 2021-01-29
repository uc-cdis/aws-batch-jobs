FROM python:3.6

COPY . /dcf-bucket-replicate

WORKDIR /dcf-bucket-replicate

RUN pip3 install awscli
RUN pip install -r requirements.txt
