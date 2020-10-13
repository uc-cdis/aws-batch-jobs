FROM quay.io/cdis/awshelper:master

COPY . /copy_to_cleversafe

WORKDIR /copy_to_cleversafe

RUN curl -O https://dl.google.com/dl/cloudsdk/channels/rapid/downloads/google-cloud-sdk-313.0.1-linux-x86_64.tar.gz
RUN tar -xvf google-cloud-sdk-313.0.1-linux-x86_64.tar.gz
RUN  ./google-cloud-sdk/install.sh --command-completion true --path-update true --usage-reporting false -q
ENTRYPOINT ["/bin/bash"]
CMD ["./batch_jobs/copy_to_cleversafe/copy_bucket.sh" ]
