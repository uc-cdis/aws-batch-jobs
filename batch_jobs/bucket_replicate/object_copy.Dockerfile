FROM amazonlinux:2 AS downloader

RUN yum install -y curl tar gzip && \
    curl -fsSL https://s3.amazonaws.com/mountpoint-s3-release/latest/x86_64/mount-s3.tar.gz \
      -o /tmp/mount-s3.tar.gz && \
    mkdir -p /tmp/mount-s3 && \
    tar -xzf /tmp/mount-s3.tar.gz -C /tmp/mount-s3 && \
    chmod +x /tmp/mount-s3

FROM amazon/aws-cli

COPY . /bucket-replicate

COPY --from=downloader /tmp/mount-s3 /bucket-replicate/mount-s3

WORKDIR /bucket-replicate

RUN mkdir -p mnt

ENTRYPOINT ["/bin/bash"]
CMD ["./batch_jobs/bucket_replicate/object_copy_job.sh"]
