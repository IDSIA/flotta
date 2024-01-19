from ferdelance.config.config import DataSourceConfiguration
from ferdelance.exceptions import InvalidAction
from ferdelance.logging import get_logger
from ferdelance.schemas.updates import UpdateData
from ferdelance.shared.actions import Action
from ferdelance.tasks.jobs.execution import Execution


LOGGER = get_logger(__name__)


class ScheduleActionService:
    def __init__(
        self,
        component_id: str,
        private_key: str,
    ) -> None:
        self.component_id: str = component_id
        self.private_key: str = private_key

    def do_nothing(self) -> Action:
        LOGGER.debug("nothing new from the server node")
        return Action.DO_NOTHING

    def start(
        self,
        scheduler_id: str,
        scheduler_url: str,
        scheduler_public_key: str,
        artifact_id: str,
        job_id: str,
        dsc: list[DataSourceConfiguration],
    ) -> Action:
        actor_handler = Execution.remote(
            self.component_id,
            artifact_id,
            job_id,
            scheduler_id,
            scheduler_url,
            scheduler_public_key,
            self.private_key,
            [d.dict() for d in dsc],
            False,
        )
        _ = actor_handler.run.remote()  # type: ignore

        return Action.DO_NOTHING

    def schedule(
        self,
        scheduler_id: str,
        scheduler_url: str,
        scheduler_public_key: str,
        update_data: UpdateData,
        dsc: list[DataSourceConfiguration],
    ) -> Action:
        action = Action[update_data.action]

        if action == Action.EXECUTE:
            return self.start(
                scheduler_id,
                scheduler_url,
                scheduler_public_key,
                update_data.artifact_id,
                update_data.job_id,
                dsc,
            )

        if action == Action.DO_NOTHING:
            return self.do_nothing()

        raise InvalidAction(f"cannot complete action={action}")
