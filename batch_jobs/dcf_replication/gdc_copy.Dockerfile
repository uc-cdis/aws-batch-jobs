ARG AZLINUX_BASE_VERSION=master
FROM quay.io/cdis/python-nginx-al:${AZLINUX_BASE_VERSION} AS base
FROM quay.io/cdis/golang:1.17-bullseye AS build-deps

ENV appname=dcf_replication

WORKDIR /${appname}

FROM base AS builder
USER root
COPY poetry.lock pyproject.toml /${appname}/
RUN poetry install -vv --no-root --without dev --no-interaction && \
    poetry show -v
COPY . /${appname}

FROM base
ENV PATH="/${appname}/.venv/bin:$PATH"
USER root

RUN yum update -y && \
    yum install -y \
        fuse \
        tar \
        gzip \
    && \
    curl -fsSL https://s3.amazonaws.com/mountpoint-s3-release/latest/x86_64/mount-s3.rpm \
        -o /tmp/mount-s3.rpm && \
    yum install -y /tmp/mount-s3.rpm && \
    rm -f /tmp/mount-s3.rpm && \
    yum clean all

# Install s5cmd
RUN curl -fsSL https://github.com/peak/s5cmd/releases/download/v2.2.2/s5cmd_2.2.2_Linux-64bit.tar.gz \
        -o /tmp/s5cmd.tar.gz && \
    tar -xzf /tmp/s5cmd.tar.gz -C /tmp && \
    mv /tmp/s5cmd /usr/local/bin/s5cmd && \
    chmod +x /usr/local/bin/s5cmd && \
    rm -f /tmp/s5cmd.tar.gz && \
    s5cmd version

RUN mkdir -p mnt
COPY --from=builder /$appname /$appname
WORKDIR /${appname}
ENTRYPOINT ["/bin/bash"]
CMD [ "/dcf_replication/batch_jobs/dcf_replication/gdc_copy_job.sh" ]
