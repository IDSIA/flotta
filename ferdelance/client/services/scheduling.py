from ferdelance.config import get_logger
from ferdelance.client.state import ClientState
from ferdelance.client.services.routes import RouteService
from ferdelance.exceptions import InvalidAction
from ferdelance.schemas.updates import UpdateToken, UpdateClientApp, UpdateExecute
from ferdelance.schemas.tasks import TaskArguments
from ferdelance.shared.actions import Action as ActionType
from ferdelance.shared.exchange import Exchange
from ferdelance.tasks.backends import get_jobs_backend

LOGGER = get_logger(__name__)


class ScheduleActionService:
    def __init__(self, config: ClientState) -> None:
        self.config: ClientState = config
        self.routes_service: RouteService = RouteService(config)

    def do_nothing(self) -> ActionType:
        LOGGER.debug("nothing new from the server")
        return ActionType.DO_NOTHING

    def update_client(self, data: UpdateClientApp) -> ActionType:
        self.routes_service.get_new_client(data)
        return ActionType.UPDATE_CLIENT

    def update_token(self, data: UpdateToken) -> ActionType:
        LOGGER.info("updating client token with a new one")
        self.config.set_token(data.token)
        return ActionType.UPDATE_TOKEN

    def start_training(self, data: UpdateExecute) -> ActionType:
        backend = get_jobs_backend()

        assert self.config.private_key_location is not None
        assert self.config.server_public_key is not None
        assert self.config.client_token is not None

        exc = Exchange()
        exc.load_key(self.config.private_key_location)

        backend.start_training(
            TaskArguments(
                private_key=exc.transfer_private_key(),
                server_url=self.config.server,
                server_public_key=self.config.server_public_key,
                token=self.config.client_token,
                datasources=[d.dict() for d in self.config.datasources],
                workdir=self.config.workdir,
                job_id=data.job_id,
                artifact_id=data.artifact_id,
            )
        )
        return ActionType.DO_NOTHING

    def start_estimate(self, data: UpdateExecute) -> ActionType:
        backend = get_jobs_backend()

        assert self.config.private_key_location is not None
        assert self.config.server_public_key is not None
        assert self.config.client_token is not None

        exc = Exchange()
        exc.load_key(self.config.private_key_location)

        backend.start_estimation(
            TaskArguments(
                private_key=exc.transfer_private_key(),
                server_url=self.config.server,
                server_public_key=self.config.server_public_key,
                token=self.config.client_token,
                datasources=[d.dict() for d in self.config.datasources],
                workdir=self.config.workdir,
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

        if action == ActionType.UPDATE_TOKEN:
            return self.update_token(UpdateToken(**data))

        if action == ActionType.UPDATE_CLIENT:
            return self.update_client(UpdateClientApp(**data))

        if action == ActionType.DO_NOTHING:
            return self.do_nothing()

        raise InvalidAction(f"cannot complete action={action}")
