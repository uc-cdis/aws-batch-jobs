FROM python:3.6

RUN pip install -r requirements.txt

COPY . /bucket-manifest

WORKDIR /bucket-manifest

ENTRYPOINT [ "python" ]
CMD [ "batch_jobs/bin/run_object_metadata_job.py" ]
