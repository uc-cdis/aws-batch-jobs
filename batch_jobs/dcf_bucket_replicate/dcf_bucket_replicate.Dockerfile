FROM FROM quay.io/cdis/python-nginx:pybase3-1.4.2

COPY . /dcf-bucket-replicate

WORKDIR /dcf-bucket-replicate

RUN pip3 install awscli
RUN pip install -r requirements.txt
