ARG AZLINUX_BASE_VERSION=master

FROM quay.io/cdis/python-nginx-al:${AZLINUX_BASE_VERSION} AS base

COPY . /bucket-manifest

WORKDIR /bucket-manifest

RUN pip install awscli

RUN poetry config virtualenvs.create false
RUN poetry install --no-dev --no-interaction --no-ansi
