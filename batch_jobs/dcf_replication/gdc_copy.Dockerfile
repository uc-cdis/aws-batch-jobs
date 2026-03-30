ARG AZLINUX_BASE_VERSION=3.13-pythonnginx

FROM quay.io/cdis/amazonlinux-base:${AZLINUX_BASE_VERSION} AS base

ENV appname=dcf_replication

WORKDIR /${appname}

FROM base AS builder

USER root

# We were copying just the poetry artifacts but poetry install expects a poetry format. Copying everything copies all the required files.
COPY . /${appname}/

# install the app dependencies (including awscli and boto3)
# requires us to run poetry lock here because the poetry.lock file on github was created by a version of poetry that is not available here
RUN poetry lock && \
    poetry install -vv --without dev --no-interaction && \
    poetry show -v

FROM base

ENV PATH="/${appname}/.venv/bin:$PATH"

USER root

RUN yum update -y && \
    yum install -y \
        fuse \
        && \
    curl -fsSL https://s3.amazonaws.com/mountpoint-s3-release/latest/x86_64/mount-s3.rpm \
        -o /tmp/mount-s3.rpm && \
    yum install -y /tmp/mount-s3.rpm && \
    rm -f /tmp/mount-s3.rpm && \
    yum clean all

RUN mkdir -p mnt

COPY --from=builder /$appname /$appname

WORKDIR /${appname}

ENTRYPOINT ["/bin/bash"]
CMD [ "/dcf_replication/batch_jobs/dcf_replication/gdc_copy_job.sh" ]
