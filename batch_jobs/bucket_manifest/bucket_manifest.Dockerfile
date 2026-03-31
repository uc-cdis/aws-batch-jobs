ARG AZLINUX_BASE_VERSION=3.13-pythonnginx

FROM quay.io/cdis/amazonlinux-base:${AZLINUX_BASE_VERSION} AS base

FROM base AS builder

USER root

COPY poetry.lock pyproject.toml /bucket-manifest/

COPY . /bucket-manifest

WORKDIR /bucket-manifest

RUN poetry install -vv --no-root --without dev --no-interaction && \
    poetry show -v

COPY --from=builder /venv /venv

ENV PATH="/bucket-manifest/.venv/bin:$PATH"
