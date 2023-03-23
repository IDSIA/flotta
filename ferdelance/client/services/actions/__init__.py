from ferdelance.client.config import Config
from ferdelance.client.services.actions.do_nothing import DoNothingAction
from ferdelance.client.services.actions.update_client import UpdateClientAction
from ferdelance.client.services.actions.update_token import UpdateTokenAction
from ferdelance.schemas.updates import UpdateToken, UpdateClientApp
from ferdelance.shared.actions import Action as ActionType

from multiprocessing import Queue

import logging

LOGGER = logging.getLogger(__name__)


class ActionService:
    def __init__(self, config: Config, train_queue: Queue, estimate_queue: Queue) -> None:
        self.config: Config = config

        self.train_queue: Queue = train_queue
        self.estimate_queue: Queue = estimate_queue

    def perform_action(self, action: ActionType, data: dict) -> ActionType:
        if action == ActionType.UPDATE_TOKEN:
            update_token_action = UpdateTokenAction(self.config, UpdateToken(**data))
            update_token_action.execute()
            return ActionType.UPDATE_TOKEN

        if action == ActionType.EXECUTE_TRAINING:
            self.train_queue.put(data)
            return ActionType.DO_NOTHING

        if action == ActionType.EXECUTE_ESTIMATE:
            self.estimate_queue.put(data)
            return ActionType.DO_NOTHING

        if action == ActionType.UPDATE_CLIENT:
            update_client_action = UpdateClientAction(self.config, UpdateClientApp(**data))
            update_client_action.execute()
            return ActionType.UPDATE_CLIENT

        if action == ActionType.DO_NOTHING:
            DoNothingAction().execute()
            return ActionType.DO_NOTHING

        # TODO: this should be an error that invalidates a job
        LOGGER.error(f"cannot complete action={action}")
        return ActionType.DO_NOTHING
