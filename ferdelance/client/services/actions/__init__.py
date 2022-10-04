from .action import Action
from .controller import ClientActionController

from .do_nothing import DoNothingAction
from .execute import ExecuteAction
from .update_client import UpdateClientAction
from .update_token import UpdateTokenAction
from .controller import ClientActionController
from ferdelance_shared.actions import Action
from ferdelance_shared.schemas import *

from ferdelance.client.config import Config
from ferdelance.client.services.routes import RouteService

import logging

LOGGER = logging.getLogger(__name__)


class ActionService:
    def __init__(self, config: Config) -> None:
        self.config: Config = config
        self.routes_service: RouteService = RouteService(config)
        self.controller: ClientActionController = ClientActionController()

    def perform_action(self, action: Action, data: dict) -> Action:
        LOGGER.info(f'action received={action}')

        if action == Action.UPDATE_TOKEN:
            update_token_action = UpdateTokenAction(self.config, UpdateToken(**data))
            self.controller.execute(update_token_action)
            return Action.UPDATE_TOKEN

        if action == Action.EXECUTE:
            execute_action = ExecuteAction(self.routes_service,  UpdateExecute(**data))
            self.controller.execute(execute_action)
            return Action.DO_NOTHING

        if action == Action.UPDATE_CLIENT:
            update_client_action = UpdateClientAction(self.routes_service, UpdateClientApp(**data))
            self.controller.execute(update_client_action)
            return Action.UPDATE_CLIENT

        if action == Action.DO_NOTHING:
            self.controller.execute(DoNothingAction())
            return Action.DO_NOTHING

        LOGGER.error(f'cannot complete action={action}')
        return Action.DO_NOTHING