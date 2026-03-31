    ARG AZLINUX_BASE_VERSION=3.13-pythonnginx

    FROM quay.io/cdis/amazonlinux-base:${AZLINUX_BASE_VERSION} AS base

    ENV appname=dcf_replication

    WORKDIR /${appname}

    FROM base AS builder

    USER root

    # copy ONLY poetry artifact, install the dependencies but not the app;
    # this will make sure that the dependencies are \

    COPY pyproject.toml /${appname}/

    RUN poetry lock

    # install the app dependencies (including awscli and boto3)
    RUN poetry install -vv --no-root --without dev --no-interaction && \
        poetry show -v

    RUN poetry install --dry-run

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
    COPY --from=builder /venv /venv

    CMD [ "/bin/bash", "-c", "/batch_jobs/dcf_replication/gdc_copy_job.sh" ]
