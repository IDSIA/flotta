from ferdelance.shared.schemas import UpdateToken

from ...config import Config
from .action import Action

import logging


LOGGER = logging.getLogger(__name__)


class UpdateTokenAction(Action):

    def __init__(self, config: Config, data: UpdateToken) -> None:
        self.config = config
        self.data = data

    def validate_input(self):
        if not isinstance(self.config, Config):
            raise ValueError(f"config parameter must be of type Config")
        if not isinstance(self.data, UpdateToken):
            raise ValueError(f"data parameter must be of type UpdateToken")

    def execute(self) -> None:
        LOGGER.info('updating client token with a new one')
        self.config.exc.set_token(self.data.token)
        self.config.dump()
