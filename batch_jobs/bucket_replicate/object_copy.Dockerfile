FROM quay.io/cdis/awshelper:master

COPY . /bucket-replicate

WORKDIR /bucket-replicate

USER root

RUN curl -fsSL https://s3.amazonaws.com/mountpoint-s3-release/latest/x86_64/mount-s3.deb \
      -o /tmp/mount-s3.deb && \
    apt-get install /tmp/mount-s3.deb -y && \
    rm -f /tmp/mount-s3.deb

RUN mkdir -p mnt

ENTRYPOINT ["/bin/bash"]
CMD ["./batch_jobs/bucket_replicate/dcf_replication.sh"]
