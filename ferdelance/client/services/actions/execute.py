from ferdelance.client.config import DataConfig
from ferdelance.client.services.actions.action import Action
from ferdelance.schemas.artifacts import Artifact
from ferdelance.schemas.client import ClientTask
from ferdelance.schemas.estimators import apply_estimator
from ferdelance.schemas.transformers import apply_transformer

from dataclasses import dataclass

import pandas as pd

import json
import logging
import os

LOGGER = logging.getLogger(__name__)


@dataclass
class ExecutionResult:
    job_id: str
    path: str
    metrics: list
    is_model: bool = False
    is_estimate: bool = False


class ExecuteAction(Action):
    def __init__(self, data: DataConfig) -> None:
        self.data = data

    def validate_input(self) -> None:
        pass

    def execute(self, task: ClientTask) -> ExecutionResult:
        job_id = task.job_id
        artifact: Artifact = task.artifact
        artifact_id = artifact.id

        if not artifact_id:
            raise ValueError("Invalid Artifact")

        LOGGER.info(f"artifact_id={artifact_id}: received new task with job_id={job_id}")

        # TODO: this should include iteration!
        working_folder = os.path.join(self.data.path_artifacts_folder(), f"{artifact_id}", f"{job_id}")

        os.makedirs(working_folder, exist_ok=True)

        path_artifact = os.path.join(working_folder, f"descriptor.json")

        with open(path_artifact, "w") as f:
            json.dump(artifact.dict(), f)

        LOGGER.info(f"artifact_id={artifact_id}: saved to {path_artifact}")

        dfs: list[pd.DataFrame] = []

        LOGGER.debug(f"artifact_id={artifact_id}: number of transformation queries={len(task.datasource_hashes)}")

        for ds_hash in task.datasource_hashes:
            # EXTRACT data from datasource
            LOGGER.info(f"artifact_id={artifact_id}: execute extraction from datasource_hash={ds_hash}")

            ds = self.data.datasources.get(ds_hash, None)
            if not ds:
                raise ValueError()

            datasource: pd.DataFrame = ds.get()  # TODO: implemented only for files

            # TRANSFORM using query
            LOGGER.info(f"artifact_id={artifact_id}: execute transformation on datasource_hash={ds_hash}")

            df = datasource.copy()

            for i, stage in enumerate(artifact.transform.stages):
                if stage.transformer is None:
                    continue

                df = apply_transformer(stage.transformer, df, working_folder, artifact_id, i)

            dfs.append(df)

        df_dataset = pd.concat(dfs)

        LOGGER.info(f"artifact_id={artifact_id}:dataset shape: {df_dataset.shape}")

        path_datasource = os.path.join(working_folder, f"dataset.csv.gz")

        df_dataset.to_csv(path_datasource, compression="gzip")

        LOGGER.info(f"artifact_id={artifact_id}: saved data to {path_datasource}")

        # do we have an estimator?
        if artifact.estimator is not None:
            LOGGER.info(f"artifact_id={artifact_id}: executing estimation")

            path_estimator = apply_estimator(artifact.estimator, df_dataset, working_folder, artifact_id)

            return ExecutionResult(
                job_id=job_id,
                path=path_estimator,
                metrics=[],
                is_estimate=True,
            )

        elif artifact.model is not None and artifact.plan is not None:
            LOGGER.info(f"artifact_id={artifact_id}: executing model training")

            # model preparation
            local_model = artifact.get_model()

            # LOAD execution plan
            plan = artifact.get_plan()

            metrics = plan.run(df_dataset, local_model, working_folder, artifact_id)

            if plan.path_model is not None:
                return ExecutionResult(
                    job_id=job_id,
                    path=plan.path_model,
                    metrics=metrics,
                    is_model=True,
                )

        raise ValueError("Invalid artifact operations")
