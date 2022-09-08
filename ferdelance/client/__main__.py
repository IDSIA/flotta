from ferdelance.client.arguments import setup_arguments
from ferdelance.client import FerdelanceClient

from dotenv import load_dotenv

import logging
import os
import sys

load_dotenv()

LOG_LEVEL = os.environ.get('LOG_LEVEL', 'INFO').upper()
logging.basicConfig(
    level=LOG_LEVEL,
    format='%(asctime)s %(name)8s %(levelname)6s %(message)s',
)

LOGGER = logging.getLogger(__name__)


if __name__ == '__main__':
    """Exit codes:
    - 0 relaunch
    - 1 update (relaunch)
    - 2 error (terminate)
    """

    args = setup_arguments()

    client = FerdelanceClient(**args)
    client.run()

    LOGGER.info('terminate application')
    sys.exit(0)
