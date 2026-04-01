ARG AZLINUX_BASE_VERSION=3.13-pythonnginx

FROM quay.io/cdis/amazonlinux-base:${AZLINUX_BASE_VERSION} AS base

ENV appname=dcf_replication

WORKDIR /${appname}

RUN chown -R gen3:gen3 /${appname}

# Builder stage
FROM base AS builder

USER root

RUN mkdir /.aws
RUN chown -R gen3:gen3 /venv
RUN chown -R gen3:gen3 /.aws

USER gen3

COPY poetry.lock pyproject.toml /${appname}/

# install the app dependencies (including awscli and boto3)
RUN poetry install -vv --no-interaction --without dev

COPY --chown=gen3:gen3 . /${appname}

RUN poetry install -vv --no-interaction --without dev

# Final stage
FROM base

COPY --from=builder /${appname} /${appname}
COPY --from=builder /venv /venv
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

CMD [ "/bin/bash", "-c", "/${appname}/batch_jobs/dcf_replication/gdc_copy_job.sh" ]
