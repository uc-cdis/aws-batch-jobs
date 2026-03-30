ARG AZLINUX_BASE_VERSION=3.13-pythonnginx

FROM quay.io/cdis/amazonlinux-base:${AZLINUX_BASE_VERSION} AS base

ENV appname=dcf_replication

WORKDIR /${appname}

FROM base AS builder

USER root

RUN poetry --version
# copy ONLY poetry artifact, install the dependencies but not the app;
# this will make sure that the dependencies are cached
COPY poetry.lock pyproject.toml /${appname}/

ENV POETRY_VIRTUALENVS_IN_PROJECT=true

# install the app dependencies (including awscli and boto3)
# RUN poetry install -vv --no-root --without dev --no-interaction && \
#     poetry show -v

RUN poetry install -vv --no-root --without dev --no-interaction && \
    poetry env info --path && \
    ls -l /dcf_replication/.venv/

# Now copy the rest of the application
COPY . /${appname}

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
