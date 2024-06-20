from flotta.logging import get_logger
import flotta

LOGGER = get_logger(__name__)

LOGGER.info(f"Using flotta version: {flotta.__version__}")
