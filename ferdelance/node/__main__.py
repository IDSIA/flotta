from ferdelance.config import config_manager, get_logger

from ferdelance.node.deployment import start_server

import ray
import requests

from time import sleep

import os

LOGGER = get_logger(f"{__package__}.{__name__}")


if __name__ == "__main__":
    os.environ["FERDELANCE_MODE"] = "server"

    config = config_manager.get()

    ray.init()

    c = start_server(config)

    # This is an horrid way to keep this script (and ray) alive...
    while True:
        sleep(config.node.healthcheck)
        try:
            res = requests.get(f"{config.node.url_deploy()}/")
            res.raise_for_status()
        except Exception as e:
            LOGGER.error(e)
            LOGGER.exception(e)
