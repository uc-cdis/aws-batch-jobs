FROM amazon/aws-cli:2.33.22

COPY . /bucket-replicate

WORKDIR /bucket-replicate

ENTRYPOINT ["/bin/bash"]
CMD ["./batch_jobs/bucket_replicate/object_copy_job.sh" ]
