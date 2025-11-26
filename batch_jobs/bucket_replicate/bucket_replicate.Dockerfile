ARG AZLINUX_BASE_VERSION=master

FROM quay.io/cdis/python-nginx-al:${AZLINUX_BASE_VERSION} AS base

COPY . /bucket-replicate

WORKDIR /bucket-replicate

RUN pip3 install awscli
RUN poetry config virtualenvs.create false
RUN poetry install --no-dev --no-interaction --no-ansi
