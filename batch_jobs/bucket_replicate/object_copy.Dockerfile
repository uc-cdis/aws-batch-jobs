FROM quay.io/cdis/awshelper:master

COPY . /bucket-replicate

WORKDIR /bucket-replicate

RUN curl -fsSL https://s3.amazonaws.com/mountpoint-s3-release/latest/x86_64/mount-s3.deb \
      -o /tmp/mount-s3.rpm && \
    apt-get install /tmp/mount-s3.deb && \
    rm -f /tmp/mount-s3.rpm

RUN mkdir -p mnt

ENTRYPOINT ["/bin/bash"]
CMD ["./batch_jobs/bucket_replicate/object_copy_job.sh"]
