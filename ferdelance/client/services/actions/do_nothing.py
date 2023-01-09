from .action import Action

import logging


LOGGER = logging.getLogger(__name__)


class DoNothingAction(Action):

    def execute(self) -> None:
        LOGGER.debug('nothing new from the server')

    def validate_input(self) -> None:
        return
