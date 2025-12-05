ARG AZLINUX_BASE_VERSION=master

FROM quay.io/cdis/python-nginx-al:${AZLINUX_BASE_VERSION} AS base

USER root

COPY poetry.lock pyproject.toml /bucket-manifest/

COPY . /bucket-manifest

WORKDIR /bucket-manifest

RUN pip install awscli

RUN poetry install -vv --no-root --without dev --no-interaction && \
    poetry show -v

ENV PATH="/bucket-manifest/.venv/bin:$PATH"

ENTRYPOINT [ "python" ]
CMD [ "batch_jobs/bin/run_object_metadata_job.py" ]
