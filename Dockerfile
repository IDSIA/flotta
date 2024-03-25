# syntax=docker/dockerfile:1.5

# This image is used for host the node.

# Build stage -----------------------------------------------------------------
FROM python:3.10 AS builder

# install virtual environment
RUN python -m venv /opt/venv

ENV PATH="/opt/venv/bin:${PATH}"

# copy source files
COPY . /ferdelance

RUN --mount=type=cache,target=/root/.cache \
    python -m pip install --upgrade pip \
    && \
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
    /ferdelance/storage/results \
    /ferdelance/logs && \
    chown -R ferdelance:ferdelance /ferdelance/

EXPOSE 1456

ENTRYPOINT ["python3", "-m", "ferdelance", "-c", "/ferdelance/config.yaml"]
