# syntax=docker/dockerfile:1.5

# This image is used for different containers with different commands
# They will be provided at compose level.

# Build stage -----------------------------------------------------------------
FROM python:3.10 AS builder

# install virtual environment
RUN python -m venv /opt/venv

ENV PATH="/opt/venv/bin:${PATH}"

RUN python -m pip install --upgrade pip

# copy source files
COPY . /ferdelance

RUN --mount=type=cache,target=/root/.cache \
    pip install "/ferdelance"

# Installation stage ----------------------------------------------------------
FROM python:3.10-slim-buster AS base

# copy built virtual environment to base
COPY --from=builder /opt/venv /opt/venv

ENV PATH="/opt/venv/bin:${PATH}"

RUN useradd -m -d /ferdelance ferdelance

# create and populate workdir
USER ferdelance
WORKDIR /ferdelance

RUN mkdir -p \ 
    /ferdelance/storage/datasources \ 
    /ferdelance/storage/artifacts \ 
    /ferdelance/storage/clients \ 
    /ferdelance/storage/results

COPY --chown=ferdelance ./script/init.sh .

RUN chmod +x ./init.sh

EXPOSE 1456

ENTRYPOINT ["/bin/bash", "init.sh"]
