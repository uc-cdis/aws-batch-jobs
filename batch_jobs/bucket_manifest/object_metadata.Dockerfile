FROM python:3.6

COPY . /bucket-manifest

WORKDIR /bucket-manifest

RUN pip install -r requirements.txt

ENTRYPOINT [ "python" ]
CMD [ "batch_jobs/bin/run_object_metadata_job.py" ]
