import logging
import sys
import os

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(name)s %(levelname)5s %(message)s',
)

LOGGER = logging.getLogger(__name__)


if __name__ == '__main__':

    if os.environ.get('update'):
        LOGGER.info('update application')
        sys.exit(1)

    if os.environ.get('install'):
        LOGGER.info('update and install packages')
        sys.exit(2)

    LOGGER.info('terminate application')
    sys.exit(0)
