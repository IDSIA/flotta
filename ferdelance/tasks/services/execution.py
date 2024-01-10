from ferdelance.config import config_manager
from ferdelance.config.config import DataSourceStorage
from ferdelance.core.environment import Environment
from ferdelance.logging import get_logger
from ferdelance.tasks.tasks import Task

import json
import pandas as pd

LOGGER = get_logger(__name__)


class ExecutionService:
    def __init__(self, task: Task, data: DataSourceStorage | None, component_id: str) -> None:
        self.task: Task = task
        self.data: DataSourceStorage | None = data

        self.component_id: str = component_id

        self.project_token: str = task.project_token
        self.artifact_id: str = task.artifact_id
        self.iteration: int = task.iteration
        self.job_id: str = task.job_id

        config = config_manager.get()

        wd = config.storage_job(self.artifact_id, self.job_id, self.iteration)

        self.env: Environment = Environment(self.artifact_id, self.project_token, task.produced_resource_id, wd)

        with open(self.env.working_dir / "task.json", "w") as f:
            json.dump(self.task.dict(), f)

    def load(self):
        if self.data is None:
            return

        # TODO: load resources from local disk!
        # Assume that the external downloader will fetch and download all the required resourced
        # from previous nodes/workers and save it to disk. This method will have to check if the
        # resource is available and use it.
        # PRO TIP: load on demand from disk what is needed when it is needed!

        dfs: list[pd.DataFrame] = []

        LOGGER.debug(f"artifact={self.artifact_id}: number of datasources={len(self.data)}")

        for hs in self.data.hashes():
            ds = self.data[hs]

            if not ds:
                LOGGER.debug(f"artifact={self.artifact_id}: datasource_hash={hs} invalid")
                continue

            if not ds.check_token(self.project_token):
                LOGGER.debug(f"artifact={self.artifact_id}: datasource_hash={hs} ignored")
                continue

            LOGGER.debug(f"artifact={self.artifact_id}: considering datasource_hash={hs}")

            datasource: pd.DataFrame = ds.get()  # TODO: implemented only for files!

            dfs.append(datasource)

        if dfs:
            self.env.df = pd.concat(dfs)

    def run(self) -> None:
        self.env = self.task.run(self.env)

        self.env.store()

        # TODO: manage error
