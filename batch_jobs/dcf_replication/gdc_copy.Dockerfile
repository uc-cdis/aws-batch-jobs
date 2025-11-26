ARG AZLINUX_BASE_VERSION=master

FROM quay.io/cdis/python-nginx-al:${AZLINUX_BASE_VERSION} AS base

COPY . /dcf_replication

WORKDIR /dcf_replication

RUN pip3 install awscli
RUN poetry config virtualenvs.create false
RUN poetry install --no-dev --no-interaction --no-ansi

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

ENTRYPOINT ["/bin/bash"]
CMD [ "/dcf_replication/batch_jobs/dcf_replication/gdc_copy_job.sh" ]
