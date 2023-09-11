from ferdelance.config import config_manager
from ferdelance.logging import get_logger
from ferdelance.node.deployment import start_node, wait_node

import ray

import os

LOGGER = get_logger(f"{__package__}.{__name__}")


if __name__ == "__main__":
    os.environ["FERDELANCE_MODE"] = "server"

    config_manager.setup()
    config = config_manager.get()

    # TODO: setup here the configuration parameters desired

    config.dump()

    ray.init()

    c = start_node(config)

    wait_node(config, c)

    ray.shutdown()
