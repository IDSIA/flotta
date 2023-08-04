from ferdelance.client.client import start_client
from ferdelance.client.arguments import setup_config_from_arguments
from ferdelance.client.exceptions import ClientExitStatus

import ray

import logging
import os
import sys

LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()

logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s %(name)8s %(levelname)6s %(message)s",
)

LOGGER = logging.getLogger(__name__)


if __name__ == "__main__":
    """Exit codes:
    - 0 relaunch
    - 1 update (relaunch)
    - 2 error (terminate)
    """

    ray.init()

    try:
        state = setup_config_from_arguments()
        exit_code = start_client(state)

    except ClientExitStatus as e:
        exit_code = e.exit_code

    LOGGER.info(f"terminated application with exit_code={exit_code}")
    sys.exit(exit_code)
