from .controller import ClientActionController

from ferdelance.client.config import Config
from ferdelance.client.services.actions.controller import ClientActionController
from ferdelance.client.services.actions.do_nothing import DoNothingAction
from ferdelance.client.services.actions.execute import ExecuteAction
from ferdelance.client.services.actions.update_client import UpdateClientAction
from ferdelance.client.services.actions.update_token import UpdateTokenAction
from ferdelance.shared.actions import Action as ActionType
from ferdelance.schemas.updates import UpdateToken, UpdateExecute, UpdateClientApp

import logging

LOGGER = logging.getLogger(__name__)


class ActionService:
    def __init__(self, config: Config) -> None:
        self.config: Config = config
        self.controller: ClientActionController = ClientActionController()

    def perform_action(self, action: ActionType, data: dict) -> ActionType:
        if action == ActionType.UPDATE_TOKEN:
            update_token_action = UpdateTokenAction(self.config, UpdateToken(**data))
            self.controller.execute(update_token_action)
            return ActionType.UPDATE_TOKEN

        if action == ActionType.EXECUTE:
            execute_action = ExecuteAction(self.config, UpdateExecute(**data))
            self.controller.execute(execute_action)
            return ActionType.DO_NOTHING

        if action == ActionType.UPDATE_CLIENT:
            update_client_action = UpdateClientAction(self.config, UpdateClientApp(**data))
            self.controller.execute(update_client_action)
            return ActionType.UPDATE_CLIENT

        if action == ActionType.DO_NOTHING:
            self.controller.execute(DoNothingAction())
            return ActionType.DO_NOTHING

        LOGGER.error(f"cannot complete action={action}")
        return ActionType.DO_NOTHING
