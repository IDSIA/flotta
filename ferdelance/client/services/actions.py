from ferdelance_shared.actions import Action
from ferdelance_shared.schemas import *

from ..config import Config
from ..services.routes import RouteService

import json
import logging
import os
import requests

LOGGER = logging.getLogger(__name__)


class ActionService:
    def __init__(self, config: Config) -> None:
        self.config: Config = config
        self.routes_service: RouteService = RouteService(config)

    def action_update_token(self, data: UpdateToken) -> None:
        LOGGER.info('updating client token with a new one')
        self.client_token = data.token
        self.config.dump()

    def action_update_client(self, data: UpdateClientApp) -> str:
        # TODO: this is something for the next iteration

        self.routes_service.get_new_client(data)

        return Action.CLIENT_UPDATE

    def action_do_nothing(self) -> str:
        LOGGER.info('nothing new from the server')
        return Action.DO_NOTHING

    def action_execute_task(self, task: UpdateExecute) -> str:
        LOGGER.info('executing new task')
        content: ArtifactTask = self.routes_service.get_task(task)

        # TODO: this is an example, execute required task when implemented

        LOGGER.info(f'received artifact_id={content.artifact_id}')

        with open(os.path.join(self.config.path_artifact_folder, f'{content.artifact_id}.json'), 'w') as f:
            json.dump(content.dict(), f)

    def perform_action(self, action: Action, data: dict) -> Action:
        LOGGER.info(f'action received={action}')

        if action == Action.UPDATE_TOKEN:
            self.action_update_token(UpdateToken(**data))

        if action == Action.EXECUTE:
            self.action_execute_task(UpdateExecute(**data))
            return Action.DO_NOTHING

        if action == Action.UPDATE_CLIENT:
            return self.action_update_client(UpdateClientApp(**data))

        if action == Action.DO_NOTHING:
            return self.action_do_nothing()

        LOGGER.error(f'cannot complete action={action}')
        return Action.DO_NOTHING
