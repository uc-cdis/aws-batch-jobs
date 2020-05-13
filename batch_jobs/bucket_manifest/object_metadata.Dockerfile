FROM python:3.6

RUN pip install boto3==1.11.11

COPY . /bucket-manifest

WORKDIR /bucket-manifest

ENTRYPOINT [ "python" ]
CMD [ "batch_jobs/bin/object_metadata_job.py" ]
