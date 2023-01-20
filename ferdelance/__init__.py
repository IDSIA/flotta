__version__ = "0.0.1"

from ferdelance.config import LOGGING_CONFIG

import logging.config

logging.config.dictConfig(LOGGING_CONFIG)
