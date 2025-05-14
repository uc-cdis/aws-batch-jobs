FROM amazon/aws-cli

COPY . /bucket-replicate

WORKDIR /bucket-replicate

RUN curl -O https://s3.amazonaws.com/mountpoint-s3-release/latest/x86_64/mount-s3.tar.gz
RUN tar -xzf mount-s3.tar.gz
RUN rm mount-s3.tar.gz
RUN chmod +x mount-s3
RUN mkdir ./mnt

ENTRYPOINT ["/bin/bash"]
CMD ["./batch_jobs/bucket_replicate/object_copy_job.sh" ]
