from ferdelance.client.config import Config
from ferdelance.client.services.actions.do_nothing import DoNothingAction
from ferdelance.client.services.actions.update_client import UpdateClientAction
from ferdelance.client.services.actions.update_token import UpdateTokenAction
from ferdelance.schemas.updates import UpdateToken, UpdateClientApp, UpdateExecute
from ferdelance.shared.actions import Action as ActionType
from ferdelance.jobs_backend import get_jobs_backend

import logging

LOGGER = logging.getLogger(__name__)


class ActionService:
    def __init__(self, config: Config) -> None:
        self.config: Config = config

    def perform_action(self, action: ActionType, data: dict) -> ActionType:
        backend = get_jobs_backend()

        if action == ActionType.UPDATE_TOKEN:
            update_token_action = UpdateTokenAction(self.config, UpdateToken(**data))
            update_token_action.execute()
            return ActionType.UPDATE_TOKEN

        if action == ActionType.EXECUTE_TRAINING:
            update_execute: UpdateExecute = UpdateExecute(**data)
            backend.start_training("", update_execute.job_id, update_execute.artifact_id)  # TODO: token? Encryption?
            return ActionType.DO_NOTHING

        if action == ActionType.EXECUTE_ESTIMATE:
            update_execute: UpdateExecute = UpdateExecute(**data)
            backend.start_estimation("", update_execute.job_id, update_execute.artifact_id)  # TODO: token? Encryption?
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
