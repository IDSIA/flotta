import logging


LOGGER = logging.getLogger(__name__)

class DoNothingAction:
    def execute(self, ) -> None:
        LOGGER.info('nothing new from the server')

