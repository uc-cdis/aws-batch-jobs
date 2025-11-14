FROM quay.io/cdis/awshelper:master

COPY . /dcf_replication

WORKDIR /dcf_replication

RUN pip3 install awscli
RUN pip install -r requirements.txt

USER root

RUN curl -fsSL https://s3.amazonaws.com/mountpoint-s3-release/latest/x86_64/mount-s3.deb \
      -o /tmp/mount-s3.deb && \
    apt-get install /tmp/mount-s3.deb -y && \
    rm -f /tmp/mount-s3.deb

RUN mkdir -p mnt

ENTRYPOINT ["/bin/bash"]
CMD [ "/dcf_replication/batch_jobs/dcf_replication/gdc_copy_job.sh" ]
