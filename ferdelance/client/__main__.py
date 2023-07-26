from ferdelance.client.client import FerdelanceClient
from ferdelance.client.arguments import setup_config_from_arguments
from ferdelance.client.exceptions import ClientExitStatus

import logging
import os
import signal
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

    def main_signal_handler(signum, frame):
        """This handler is used to gracefully stop when ctrl-c is hit in the terminal."""
        client.stop_loop()

    signal.signal(signal.SIGINT, main_signal_handler)
    signal.signal(signal.SIGTERM, main_signal_handler)

    try:
        conf = setup_config_from_arguments()
        client = FerdelanceClient(conf)
        exit_code = client.run()

    except ClientExitStatus as e:
        exit_code = e.exit_code

    LOGGER.info(f"terminated application with exit_code={exit_code}")
    sys.exit(exit_code)
