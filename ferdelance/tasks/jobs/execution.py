from ferdelance.client.config import DataConfig
from ferdelance.schemas.artifacts import Artifact
from ferdelance.schemas.estimators import apply_estimator
from ferdelance.schemas.transformers import apply_transformer
from ferdelance.schemas.tasks import TaskParameters, ExecutionResult

import pandas as pd

import json
import logging
import os

LOGGER = logging.getLogger(__name__)


def setup(artifact: Artifact, job_id: str, data: DataConfig) -> str:
    if not artifact.id:
        raise ValueError("Invalid Artifact")

    LOGGER.info(f"artifact.id={artifact.id}: received new task with job_id={job_id}")

    # TODO: this should include iteration!
    working_folder = os.path.join(data.path_artifacts_folder(), f"{artifact.id}", f"{job_id}")

    os.makedirs(working_folder, exist_ok=True)

    path_artifact = os.path.join(working_folder, "descriptor.json")

    with open(path_artifact, "w") as f:
        json.dump(artifact.dict(), f)

    LOGGER.info(f"artifact.id={artifact.id}: saved to {path_artifact}")

    return working_folder


def apply_transform(
    artifact: Artifact,
    task: TaskParameters,
    data: DataConfig,
    working_folder: str,
) -> pd.DataFrame:
    dfs: list[pd.DataFrame] = []

    datasource_hashes: list[str] = task.content_ids

    LOGGER.debug(f"artifact.id={artifact.id}: number of transformation queries={len(datasource_hashes)}")

    for ds_hash in datasource_hashes:
        # EXTRACT data from datasource
        LOGGER.info(f"artifact.id={artifact.id}: execute extraction from datasource_hash={ds_hash}")

        ds = data.datasources.get(ds_hash, None)
        if not ds:
            raise ValueError()

        datasource: pd.DataFrame = ds.get()  # TODO: implemented only for files

        # TRANSFORM using query
        LOGGER.info(f"artifact.id={artifact.id}: execute transformation on datasource_hash={ds_hash}")

        df = datasource.copy()

        for i, stage in enumerate(artifact.transform.stages):
            if stage.transformer is None:
                continue

            df = apply_transformer(stage.transformer, df, working_folder, artifact.id, i)

        dfs.append(df)

    df_dataset = pd.concat(dfs)

    LOGGER.info(f"artifact.id={artifact.id}:dataset shape: {df_dataset.shape}")

    path_datasource = os.path.join(working_folder, "dataset.csv.gz")

    df_dataset.to_csv(path_datasource, compression="gzip")

    LOGGER.info(f"artifact.id={artifact.id}: saved data to {path_datasource}")

    return df_dataset


def run_training(data: DataConfig, task: TaskParameters) -> ExecutionResult:
    job_id = task.job_id
    artifact: Artifact = task.artifact

    working_folder = setup(artifact, job_id, data)

    df_dataset = apply_transform(artifact, task, data, working_folder)

    if artifact.model is not None and artifact.plan is None:
        raise ValueError("Invalid artifact training")  # TODO: manage this!

    LOGGER.info(f"artifact.id={artifact.id}: executing model training")

    # model preparation
    local_model = artifact.get_model()

    # LOAD execution plan
    plan = artifact.get_plan()

    metrics = plan.run(df_dataset, local_model, working_folder, artifact.id)

    if plan.path_model is None:
        raise ValueError("Model path not set!")  # TODO: manage this!

    return ExecutionResult(
        job_id=job_id,
        path=plan.path_model,
        metrics=metrics,
        is_model=True,
    )


def run_estimate(data: DataConfig, task: TaskParameters) -> ExecutionResult:
    job_id = task.job_id
    artifact: Artifact = task.artifact
    artifact.id = artifact.id

    working_folder = setup(artifact, job_id, data)

    df_dataset = apply_transform(artifact, task, data, working_folder)

    if artifact.estimator is None:
        raise ValueError("Artifact is not an estimation!")  # TODO: manage this!

    LOGGER.info(f"artifact.id={artifact.id}: executing estimation")

    path_estimator = apply_estimator(artifact.estimator, df_dataset, working_folder, artifact.id)

    return ExecutionResult(
        job_id=job_id,
        path=path_estimator,
        metrics=[],
        is_estimate=True,
    )
