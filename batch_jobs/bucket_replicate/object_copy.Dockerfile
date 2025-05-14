FROM amazonlinux:2 AS downloader

RUN yum install -y curl tar
RUN curl -fsSL https://s3.amazonaws.com/mountpoint-s3-release/latest/x86_64/mount-s3.tar.gz \
      -o ./mount-s3.tar.gz
RUN tar -xzf ./mount-s3.tar.gz -C ./ && \
    chmod +x ./mount-s3

FROM amazon/aws-cli

COPY --from=downloader mount-s3 /usr/local/bin/mount-s3

COPY . /bucket-replicate
WORKDIR /bucket-replicate

RUN mkdir -p mnt

ENTRYPOINT ["/bin/bash"]
CMD ["./batch_jobs/bucket_replicate/object_copy_job.sh"]
