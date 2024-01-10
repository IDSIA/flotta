from ferdelance.config import config_manager
from ferdelance.logging import get_logger, get_log_formatter
from ferdelance.node.deployment import start_node, wait_node

import logging
import ray


LOGGER = get_logger(f"{__package__}.{__name__}")


if __name__ == "__main__":
    config_manager.setup()
    config = config_manager.get()
    config.dump()

    ray.init()

    # set ray loggers
    for log_name in ("ray", "ray.serve"):
        logger = logging.getLogger(log_name)
        for h in logger.handlers:
            h.setFormatter(get_log_formatter())

    try:
        c = start_node(config)

        wait_node(config, c)

    except Exception as e:
        LOGGER.exception(e)

    ray.shutdown()
