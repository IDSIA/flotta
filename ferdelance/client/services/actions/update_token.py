from ferdelance.client.config import Config
from ferdelance.client.services.actions.action import Action
from ferdelance.schemas.updates import UpdateToken

import logging


LOGGER = logging.getLogger(__name__)


class UpdateTokenAction(Action):
    def __init__(self, config: Config, data: UpdateToken) -> None:
        self.config = config
        self.data = data

    def execute(self) -> None:
        LOGGER.info("updating client token with a new one")
        self.config.set_token(self.data.token)
