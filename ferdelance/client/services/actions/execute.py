from ferdelance_shared.schemas import Artifact, UpdateExecute, QueryFeature
from ferdelance_shared.operations import Operations
from ...config import Config
from ...models import model_creator
from ..routes import RouteService
from .action import Action

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
        ...

    def execute(self) -> None:

        artifact: Artifact = self.routes_service.get_task(self.update_execute)
        artifact_id = artifact.artifact_id

        if artifact_id is None:
            raise ValueError('Invalid Artifact')

        LOGGER.info(f'received artifact_id={artifact.artifact_id}')

        working_folder = os.path.join(self.config.path_artifact_folder, f'{artifact_id}')

        os.makedirs(working_folder, exist_ok=True)

        path_artifact = os.path.join(working_folder, f'descriptor.json')

        with open(path_artifact, 'w') as f:
            json.dump(artifact.dict(), f)

        LOGGER.info(f'saved artifact_id={artifact_id} to {path_artifact}')

        dfs: list[pd.DataFrame] = []

        LOGGER.info(f'number of selection query: {len(artifact.dataset.queries)}')

        for query in artifact.dataset.queries:
            # LOAD
            LOGGER.info(f"EXECUTE -  LOAD {query.datasource_name}")

            ds = self.config.datasources.get(query.datasource_name)
            if not ds:
                raise ValueError()

            datasource: pd.DataFrame = ds.get()  # not yet implemented, but should return a pd df

            # SELECT
            LOGGER.info(f"datasource_id={query.datasource_name}: selecting")

            selected_features: list[str] = []
            for sf in query.features:
                name = sf.feature_name
                if name not in datasource.columns:
                    LOGGER.warn(f'feature_name={name} not found in data source')
                else:
                    selected_features.append(name)

            datasource = datasource[selected_features]

            LOGGER.info(f'selected data shape: {datasource.shape}')

            # FILTER
            LOGGER.info(f"datasource_id={query.datasource_name}: filtering")

            df_filtered = datasource.copy()

            for query_filter in query.filters:

                # TODO: accumulate all filters in single huge boolean vector

                feature: str = query_filter.feature.feature_name
                operation: Operations = Operations[query_filter.operation]
                parameter: str = query_filter.parameter

                apply_filter = {
                    Operations.NUM_LESS_THAN: lambda df: df[df[feature] < float(parameter)],
                    Operations.NUM_LESS_EQUAL: lambda df: df[df[feature] <= float(parameter)],
                    Operations.NUM_GREATER_THAN: lambda df: df[df[feature] > float(parameter)],
                    Operations.NUM_GREATER_EQUAL: lambda df: df[df[feature] >= float(parameter)],
                    Operations.NUM_EQUALS: lambda df: df[df[feature] == float(parameter)],
                    Operations.NUM_NOT_EQUALS: lambda df: df[df[feature] != float(parameter)],

                    Operations.OBJ_LIKE: lambda df: df[df[feature] == parameter],
                    Operations.OBJ_NOT_LIKE: lambda df: df[df[feature] != parameter],

                    Operations.TIME_BEFORE: lambda df: df[df[feature] < pd.to_datetime(parameter)],
                    Operations.TIME_AFTER: lambda df: df[df[feature] > pd.to_datetime(parameter)],
                    Operations.TIME_EQUALS: lambda df: df[df[feature] == pd.to_datetime(parameter)],
                    Operations.TIME_NOT_EQUALS: lambda df: df[df[feature] != pd.to_datetime(parameter)],
                }

                df_filtered = apply_filter[operation](df_filtered)

                LOGGER.info(f"Applying {operation}({parameter}) on {feature}")

                LOGGER.info(f'filtered data shape: {df_filtered.shape}')

            # TRANSFORM
            LOGGER.info(f"datasource_id={query.datasource_name}: transforming")

            # TODO

            # TERMINATE
            LOGGER.info(f"datasource_id={query.datasource_name}: terminated")

            # TODO

            dfs.append(df_filtered)

        df_dataset = pd.concat(dfs)

        LOGGER.info(f'dataset shape: {df_dataset.shape}')

        path_datasource = os.path.join(working_folder, f'dataset.csv.gz')

        df_dataset.to_csv(path_datasource, compression='gzip')

        LOGGER.info(f'saved artifact_id={artifact_id} data to {path_datasource}')

        # dataset preparation
        label = artifact.dataset.label
        val_p = artifact.dataset.val_percentage
        test_p = artifact.dataset.test_percentage

        if label is None:
            msg = 'label is not defined!'
            LOGGER.error(msg)
            raise ValueError(msg)

        if label not in df_dataset.columns:
            msg = f'label {label} not found in data source!'
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

        path_model = os.path.join(working_folder, f'{artifact_id}_model.pkl')
        local_model.save(path_model)

        LOGGER.info(f'saved artifact_id={artifact_id} model to {path_model}')

        # model test
        if X_ts is not None and Y_ts is not None:
            metrics = local_model.eval(X_ts, Y_ts)
            metrics.source = 'test'
            metrics.artifact_id = artifact_id
            self.routes_service.post_metrics(metrics)

        # model validation
        if X_val is not None and Y_val is not None:
            metrics = local_model.eval(X_val, Y_val)
            metrics.source = 'val'
            metrics.artifact_id = artifact_id
            self.routes_service.post_metrics(metrics)

        self.routes_service.post_model(artifact_id, path_model)
