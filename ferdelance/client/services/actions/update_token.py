from ferdelance.client.config import Config

import logging

from ferdelance_shared.schemas import UpdateToken

LOGGER = logging.getLogger(__name__)

class UpdateTokenAction:

    def __init__(self, config: Config, data: UpdateToken) -> None:
        self.config = config
        self.data = data
        self._validate_input(self.config, self.data)

    def _validate_input(config, data):
        if not isinstance(config, Config):
            raise ValueError(f"config parameter must be of type Config")
        elif not isinstance(data, UpdateToken):
            raise ValueError(f"data parameter must be of type UpateToken")


    def execute(self, ) -> None:
        LOGGER.info('updating client token with a new one')
        self.config.client_token = self.data.token
        self.config.dump()