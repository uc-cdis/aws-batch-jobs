FROM amazon/aws-cli
WORKDIR /home/ubuntu
COPY . /home/ubuntu/copy_to_cleversafe

RUN wget https://dl.google.com/dl/cloudsdk/channels/rapid/downloads/google-cloud-sdk-313.0.1-linux-x86_64.tar.gz
RUN tar -xvf google-cloud-sdk-313.0.1-linux-x86_64.tar.gz
RUN ./google-cloud-sdk/install.sh --command-completion true --path-update true --usage-reporting false -q

WORKDIR /home/ubuntu/copy_to_cleversafe
ENTRYPOINT ["/bin/bash"]
CMD ["batch_jobs/copy_to_cleversafe/object_copy.sh" ]
