from ferdelance.logging import get_logger
import ferdelance

LOGGER = get_logger(__name__)

LOGGER.info(f"Using ferdelance version: {ferdelance.__version__}")
