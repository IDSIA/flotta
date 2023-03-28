__version__ = "1.0.0"

from ferdelance.config import LOGGING_CONFIG

import logging.config

logging.config.dictConfig(LOGGING_CONFIG)
