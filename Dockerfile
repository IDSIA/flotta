# Build stage
FROM python:3.10 AS builder

# install virtual environment
RUN python -m venv /opt/venv

ENV PATH="/opt/venv/bin:${PATH}"

# copy and install shared library
COPY federated-learning-shared/ /federated-learning-shared/

RUN pip install --no-cache-dir /federated-learning-shared/

# copy config file and install dependencies
COPY setup.cfg /

RUN cd / && \
    python -c "import configparser; c = configparser.ConfigParser(); c.read('setup.cfg'); print(c['options']['install_requires'])" | xargs pip install

# Installation stage
FROM python:3.10-slim-buster AS base

# copy built virtual environment to base
COPY --from=builder /opt/venv /opt/venv

ENV PATH="/opt/venv/bin:${PATH}"

# create and populate workdir
WORKDIR /ferdelance

COPY . /ferdelance

# install application
RUN pip install --no-cache-dir .

CMD ["uvicorn", "ferdelance.server.api:api", "--host", "0.0.0.0", "--port", "1456"]
