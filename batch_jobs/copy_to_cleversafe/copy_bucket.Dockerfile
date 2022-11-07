FROM python:3.6

COPY . /bucket-manifest



RUN wget https://dl.google.com/dl/cloudsdk/channels/rapid/downloads/google-cloud-sdk-313.0.1-linux-x86_64.tar.gz
RUN tar -xvf google-cloud-sdk-313.0.1-linux-x86_64.tar.gz
RUN ./google-cloud-sdk/install.sh --command-completion true --path-update true --usage-reporting false -q
RUN pip3 install google-cloud-storage
RUN pip3 install install google-auth
WORKDIR /bucket-manifest
RUN pip3 install awscli
RUN pip install -r requirements.txt
