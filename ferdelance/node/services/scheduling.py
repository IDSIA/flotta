from ferdelance.config.config import DataSourceConfiguration
from ferdelance.exceptions import InvalidAction
from ferdelance.logging import get_logger
from ferdelance.schemas.tasks import TaskArguments
from ferdelance.schemas.updates import UpdateData
from ferdelance.shared.actions import Action
from ferdelance.tasks.backends import get_jobs_backend

LOGGER = get_logger(__name__)


class ScheduleActionService:
    def __init__(
        self,
        component_id: str,
        private_key: str,
        workdir: str = "",
    ) -> None:
        self.component_id: str = component_id
        self.private_key: str = private_key
        self.workdir: str = workdir

    def do_nothing(self) -> Action:
        LOGGER.debug("nothing new from the server node")
        return Action.DO_NOTHING

    def start_training(
        self,
        remote_url: str,
        remote_public_key: str,
        artifact_id: str,
        job_id: str,
        dsc: list[DataSourceConfiguration],
    ) -> Action:
        backend = get_jobs_backend()

        backend.start_training(
            TaskArguments(
                component_id=self.component_id,
                private_key=self.private_key,
                node_url=remote_url,
                node_public_key=remote_public_key,
                datasources=[d.dict() for d in dsc],
                workdir=self.workdir,
                job_id=job_id,
                artifact_id=artifact_id,
            )
        )
        return Action.DO_NOTHING

    def start_estimate(
        self,
        remote_url: str,
        remote_public_key: str,
        artifact_id: str,
        job_id: str,
        dsc: list[DataSourceConfiguration],
    ) -> Action:
        backend = get_jobs_backend()

        backend.start_estimation(
            TaskArguments(
                component_id=self.component_id,
                private_key=self.private_key,
                node_url=remote_url,
                node_public_key=remote_public_key,
                datasources=[d.dict() for d in dsc],
                workdir=self.workdir,
                job_id=job_id,
                artifact_id=artifact_id,
            )
        )
        return Action.DO_NOTHING

    def schedule(
        self,
        remote_url: str,
        remote_public_key: str,
        update_data: UpdateData,
        dsc: list[DataSourceConfiguration],
    ) -> Action:
        action = Action[update_data.action]

        if action == Action.EXECUTE_TRAINING:
            return self.start_training(
                remote_url,
                remote_public_key,
                update_data.artifact_id,
                update_data.job_id,
                dsc,
            )

        if action == Action.EXECUTE_ESTIMATE:
            return self.start_estimate(
                remote_url,
                remote_public_key,
                update_data.artifact_id,
                update_data.job_id,
                dsc,
            )

        if action == Action.DO_NOTHING:
            return self.do_nothing()

        raise InvalidAction(f"cannot complete action={action}")
