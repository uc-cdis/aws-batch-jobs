ARG AZLINUX_BASE_VERSION=master

FROM quay.io/cdis/python-nginx-al:${AZLINUX_BASE_VERSION} AS base

ENV appname=dcf_replication

WORKDIR /${appname}

RUN chown -R gen3:gen3 /${appname}

FROM base AS builder

USER gen3

# copy ONLY poetry artifact, install the dependencies but not the app;
# this will make sure that the dependencies are cached
COPY poetry.lock pyproject.toml /${appname}/

# install the app dependencies (including awscli and boto3)
RUN poetry install -vv --no-root --without dev --no-interaction && \
    poetry show -v

# Now copy the rest of the application
COPY --chown=gen3:gen3 . /${appname}

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

USER gen3

COPY --chown=gen3:gen3 --from=builder /$appname /$appname

# Verify installation
RUN python3 -c "import boto3; print(f'boto3 version: {boto3.__version__}')" && \
    aws --version

WORKDIR /${appname}

ENTRYPOINT ["/bin/bash"]
CMD [ "/dcf_replication/batch_jobs/dcf_replication/gdc_copy_job.sh" ]
