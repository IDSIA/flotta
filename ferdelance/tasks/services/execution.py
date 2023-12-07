from typing import Any

from ferdelance.config import config_manager
from ferdelance.config.config import DataSourceStorage
from ferdelance.core.environment import Environment
from ferdelance.logging import get_logger
from ferdelance.tasks.tasks import Task

import pickle
import json
import os
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

        self.env: Environment = Environment(self.artifact_id, self.project_token)

        self.working_folder: str = ""

    def setup(self) -> None:
        config = config_manager.get()

        # creating working folders
        self.working_folder = os.path.join(
            config.storage_artifact(self.artifact_id, self.iteration),
            f"{self.job_id}",
        )

        os.makedirs(self.working_folder, exist_ok=True)

        path_artifact = os.path.join(self.working_folder, "task.json")

        with open(path_artifact, "w") as f:
            json.dump(self.task.dict(), f)

    def load(self):
        if self.data is None:
            return

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

        self.env.df = pd.concat(dfs)  # TODO: do we want it to be loaded in df?

    def add_resource(self, resource_id: str, resource: Any) -> None:
        self.env[resource_id] = resource

    def run(self) -> None:
        self.env = self.task.run(self.env)

        path = os.path.join(self.working_folder, "local_model.pkl")
        self.env["resource_path"] = path

        with open(path, "wb") as f:
            pickle.dump(self.env["local_model"], f)

        self.env.produced_resource_path = path

        # TODO: manage error
