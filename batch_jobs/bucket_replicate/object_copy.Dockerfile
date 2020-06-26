FROM amazon/aws-cli

COPY . /bucket-replicate

WORKDIR /bucket-replicate

CMD [ "bash"] ["./batch_jobs/bucket_replicate/object_copy_job.sh" ]
