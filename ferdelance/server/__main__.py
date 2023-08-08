from ferdelance.config import config_manager

from ferdelance.server.deployment import start_server

import ray
import requests

from time import sleep

import logging

LOGGER = logging.getLogger(f"{__package__}.{__name__}")


if __name__ == "__main__":
    config = config_manager.get()

    ray.init()

    c = start_server(config)

    # This is an horrid way to keep this script (and ray) alive...
    while True:
        sleep(config.server.healthcheck)
        try:
            res = requests.get(f"{config.server.url()}/")
            res.raise_for_status()
        except Exception:
            ...
