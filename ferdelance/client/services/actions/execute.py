from ferdelance.client.config import Config
from ferdelance.client.services.actions.action import Action
from ferdelance.client.services.routes import RouteService
from ferdelance.schemas.artifacts import Artifact
from ferdelance.schemas.client import ClientTask
from ferdelance.schemas.models import model_creator
from ferdelance.schemas.transformers import apply_transformer
from ferdelance.schemas.updates import UpdateExecute

import pandas as pd

import json
import logging
import os

LOGGER = logging.getLogger(__name__)


class ExecuteAction(Action):
    def __init__(self, config: Config, update_execute: UpdateExecute) -> None:
        self.config = config
        self.routes_service: RouteService = RouteService(config)
        self.update_execute = update_execute

    def validate_input(self) -> None:
        pass

    def execute(self) -> None:

        task: ClientTask = self.routes_service.get_task(self.update_execute)
        artifact: Artifact = task.artifact
        artifact_id = artifact.artifact_id

        if artifact_id is None:
            raise ValueError("Invalid Artifact")

        LOGGER.info(f"received artifact_id={artifact.artifact_id}")

        working_folder = os.path.join(self.config.path_artifact_folder(), f"{artifact_id}")

        os.makedirs(working_folder, exist_ok=True)

        path_artifact = os.path.join(working_folder, f"descriptor.json")

        with open(path_artifact, "w") as f:
            json.dump(artifact.dict(), f)

        LOGGER.info(f"saved artifact_id={artifact_id} to {path_artifact}")

        dfs: list[pd.DataFrame] = []

        LOGGER.info(f"number of selection query: {len(task.datasource_hashes)}")

        for ds_hash in task.datasource_hashes:
            # EXTRACT data from datasource
            LOGGER.info(f"EXECUTE Extract from datasource_has={ds_hash}")

            ds = self.config.datasources.get(ds_hash, None)
            if not ds:
                raise ValueError()

            datasource: pd.DataFrame = ds.get()  # TODO: implemented only for files

            # TRANSFORM using query
            LOGGER.info(f"EXECUTE Transform datasource_hash={ds_hash}")

            df = datasource.copy()

            for stage in artifact.transform.stages:
                if stage.transformer is None:
                    continue

                df = apply_transformer(stage.transformer, df)

            dfs.append(df)

        df_dataset = pd.concat(dfs)

        LOGGER.info(f"dataset shape: {df_dataset.shape}")

        path_datasource = os.path.join(working_folder, f"dataset.csv.gz")

        df_dataset.to_csv(path_datasource, compression="gzip")

        LOGGER.info(f"saved artifact_id={artifact_id} data to {path_datasource}")

        # model preparation
        local_model = model_creator(artifact.model)

        # LOAD execution plan
        plan = artifact.load

        if plan is not None:
            plan.load(df_dataset, local_model, working_folder, artifact_id)

            for m in plan._metrics:
                self.routes_service.post_metrics(m)

            if plan._path_model is not None:
                self.routes_service.post_model(artifact_id, plan._path_model)
