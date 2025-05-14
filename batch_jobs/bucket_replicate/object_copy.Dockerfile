FROM amazon/aws-cli

COPY . /bucket-replicate

WORKDIR /bucket-replicate

RUN wget https://s3.amazonaws.com/mountpoint-s3-release/latest/x86_64/mount-s3.tar.gz && \
    tar -xzf mount-s3.tar.gz && \
    rm mount-s3.tar.gz && \
    chmod +x mount-s3 && \
    mkdir ./mnt

ENTRYPOINT ["/bin/bash"]
CMD ["./batch_jobs/bucket_replicate/object_copy_job.sh" ]
