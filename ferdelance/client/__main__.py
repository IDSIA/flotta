from ferdelance.config import config_manager
from ferdelance.logging import get_logger
from ferdelance.client.client import start_client
from ferdelance.client.exceptions import ClientExitStatus

import ray

import os
import sys

LOGGER = get_logger(f"{__package__}.{__name__}")


if __name__ == "__main__":
    """Exit codes:
    - 0 relaunch
    - 1 update (relaunch)
    - 2 error (terminate)
    """

    os.environ["FERDELANCE_MODE"] = "client"

    config = config_manager.get()
    leave = config_manager.leave()

    ray.init()

    try:
        exit_code = start_client(config, leave)

    except ClientExitStatus as e:
        exit_code = e.exit_code

    LOGGER.info(f"terminated application with exit_code={exit_code}")
    sys.exit(exit_code)
