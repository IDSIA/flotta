from ferdelance.client.state import State
from ferdelance.exceptions import InvalidAction
from ferdelance.logging import get_logger
from ferdelance.schemas.updates import UpdateExecute
from ferdelance.schemas.tasks import TaskArguments
from ferdelance.shared.actions import Action as ActionType
from ferdelance.shared.exchange import Exchange
from ferdelance.tasks.backends import get_jobs_backend

LOGGER = get_logger(__name__)


class ScheduleActionService:
    def __init__(self, state: State) -> None:
        self.state: State = state

    def do_nothing(self) -> ActionType:
        LOGGER.debug("nothing new from the server node")
        return ActionType.DO_NOTHING

    def update_client(self) -> ActionType:
        return ActionType.UPDATE_CLIENT

    def start_training(self, data: UpdateExecute) -> ActionType:
        backend = get_jobs_backend()

        assert self.state.private_key_location is not None
        assert self.state.node_public_key is not None

        exc = Exchange()
        exc.load_key(self.state.private_key_location)

        backend.start_training(
            TaskArguments(
                private_key=exc.transfer_private_key(),
                server_url=self.state.server,
                server_public_key=self.state.node_public_key,
                datasources=[d.dict() for d in self.state.datasources],
                workdir=self.state.workdir,
                job_id=data.job_id,
                artifact_id=data.artifact_id,
            )
        )
        return ActionType.DO_NOTHING

    def start_estimate(self, data: UpdateExecute) -> ActionType:
        backend = get_jobs_backend()

        assert self.state.private_key_location is not None
        assert self.state.node_public_key is not None

        exc = Exchange()
        exc.load_key(self.state.private_key_location)

        backend.start_estimation(
            TaskArguments(
                private_key=exc.transfer_private_key(),
                server_url=self.state.server,
                server_public_key=self.state.node_public_key,
                datasources=[d.dict() for d in self.state.datasources],
                workdir=self.state.workdir,
                job_id=data.job_id,
                artifact_id=data.artifact_id,
            )
        )
        return ActionType.DO_NOTHING

    def schedule(self, action: ActionType, data: dict) -> ActionType:
        if action == ActionType.EXECUTE_TRAINING:
            return self.start_training(UpdateExecute(**data))

        if action == ActionType.EXECUTE_ESTIMATE:
            return self.start_estimate(UpdateExecute(**data))

        if action == ActionType.UPDATE_CLIENT:
            return self.update_client()

        if action == ActionType.DO_NOTHING:
            return self.do_nothing()

        raise InvalidAction(f"cannot complete action={action}")
