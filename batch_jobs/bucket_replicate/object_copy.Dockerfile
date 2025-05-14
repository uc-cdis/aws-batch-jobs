FROM amazon/aws-cli

COPY . /bucket-replicate

WORKDIR /bucket-replicate

RUN curl -fsSL https://s3.amazonaws.com/mountpoint-s3-release/latest/x86_64/mount-s3.rpm \
      -o /tmp/mount-s3.rpm && \
    yum install -y /tmp/mount-s3.rpm && \
    rm -f /tmp/mount-s3.rpm

RUN mkdir -p mnt

ENTRYPOINT ["/bin/bash"]
CMD ["./batch_jobs/bucket_replicate/object_copy_job.sh"]
