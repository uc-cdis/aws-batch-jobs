ARG AZLINUX_BASE_VERSION=3.13-pythonnginx

FROM quay.io/cdis/amazonlinux-base:${AZLINUX_BASE_VERSION} AS base

ENV appname=dcf_replication

WORKDIR /${appname}

FROM base AS builder
USER root
COPY poetry.lock pyproject.toml README.md __init__.py /${appname}/

RUN python -m venv /${appname}/.venv && \
    /${appname}/.venv/bin/pip install --upgrade pip

RUN poetry config virtualenvs.in-project true && \
    poetry lock && \
    poetry install --without dev --no-interaction --no-root

COPY . /${appname}/

FROM base
ENV PATH="/${appname}/.venv/bin:$PATH"
USER root

RUN yum update -y && \
    yum install -y fuse && \
    curl -fsSL https://s3.amazonaws.com/mountpoint-s3-release/latest/x86_64/mount-s3.rpm \
      -o /tmp/mount-s3.rpm && \
    yum install -y /tmp/mount-s3.rpm && \
    rm -f /tmp/mount-s3.rpm && \
    yum clean all

RUN mkdir -p mnt
COPY --from=builder /${appname} /${appname}   # .venv is now inside here
WORKDIR /${appname}
ENTRYPOINT ["/bin/bash"]
CMD ["/dcf_replication/batch_jobs/dcf_replication/gdc_copy_job.sh"]
