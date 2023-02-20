from ferdelance.client.config import Config
from ferdelance.client.services.actions.action import Action
from ferdelance.client.services.routes import RouteService
from ferdelance.schemas.artifacts import Artifact
from ferdelance.schemas.client import ClientTask
from ferdelance.schemas.models import model_creator
from ferdelance.schemas.updates import UpdateExecute
from ferdelance.schemas.queries import Query
from ferdelance.schemas.transformers import apply_transformer

from sklearn.model_selection import train_test_split

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

        # LOAD execution plan
        label = artifact.label
        val_p = artifact.dataset.val_percentage
        test_p = artifact.dataset.test_percentage

        if label is None:
            msg = "label is not defined!"
            LOGGER.error(msg)
            raise ValueError(msg)

        if label not in df_dataset.columns:
            msg = f"label {label} not found in data source!"
            LOGGER.error(msg)
            raise ValueError(msg)

        X_tr = df_dataset.drop(label, axis=1).values
        Y_tr = df_dataset[label].values

        X_ts, Y_ts = None, None
        X_val, Y_val = None, None

        if val_p > 0.0:
            X_tr, X_val, Y_tr, Y_val = train_test_split(X_tr, Y_tr, test_size=val_p)

        if test_p > 0.0:
            X_tr, X_ts, Y_tr, Y_ts = train_test_split(X_tr, Y_tr, test_size=test_p)

        # model preparation
        local_model = model_creator(artifact.model)

        # model training
        local_model.train(X_tr, Y_tr)

        path_model = os.path.join(working_folder, f"{artifact_id}_model.pkl")
        local_model.save(path_model)

        LOGGER.info(f"saved artifact_id={artifact_id} model to {path_model}")

        # model test
        if X_ts is not None and Y_ts is not None:
            metrics = local_model.eval(X_ts, Y_ts)
            metrics.source = "test"
            metrics.artifact_id = artifact_id
            self.routes_service.post_metrics(metrics)

        # model validation
        if X_val is not None and Y_val is not None:
            metrics = local_model.eval(X_val, Y_val)
            metrics.source = "val"
            metrics.artifact_id = artifact_id
            self.routes_service.post_metrics(metrics)

        self.routes_service.post_model(artifact_id, path_model)
