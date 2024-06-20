# syntax=docker/dockerfile:1.5

# This image is used for host the node.

# Build stage -----------------------------------------------------------------
FROM python:3.10 AS builder

# install virtual environment
RUN python -m venv /opt/venv

ENV PATH="/opt/venv/bin:${PATH}"

# copy source files
COPY . /flotta

RUN --mount=type=cache,target=/root/.cache \
    python -m pip install --upgrade pip \
    && \
    pip install "/flotta"

# Installation stage ----------------------------------------------------------
FROM python:3.10-slim-buster AS base

# copy built virtual environment to base
COPY --from=builder /opt/venv /opt/venv

ENV PATH="/opt/venv/bin:${PATH}"

RUN useradd -m -d /flotta flotta

# create and populate workdir
USER flotta
WORKDIR /flotta

RUN mkdir -p \ 
    /flotta/storage/datasources \ 
    /flotta/storage/artifacts \ 
    /flotta/storage/clients \ 
    /flotta/storage/results \
    /flotta/logs && \
    chown -R flotta:flotta /flotta/

EXPOSE 1456

ENTRYPOINT ["python3", "-m", "flotta", "-c", "/flotta/config.yaml"]
