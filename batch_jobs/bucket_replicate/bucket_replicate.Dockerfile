ARG AZLINUX_BASE_VERSION=3.13-pythonnginx

FROM quay.io/cdis/amazonlinux-base:${AZLINUX_BASE_VERSION} AS base

FROM base AS builder

USER root

COPY poetry.lock pyproject.toml /bucket-replicate/

COPY . /bucket-replicate

WORKDIR /bucket-replicate

RUN poetry install -vv --no-root --without dev --no-interaction && \
    poetry show -v

ENV PATH="/bucket-replicate/.venv/bin:$PATH"
