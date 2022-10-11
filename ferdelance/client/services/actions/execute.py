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

        LOGGER.info('received artifact_id={artifact.artifact_id}')

        working_folder = os.path.join(self.config.path_artifact_folder, f'{artifact_id}')

        os.makedirs(working_folder, exist_ok=True)

        path_artifact = os.path.join(working_folder, f'{artifact_id}.json')

        with open(path_artifact, 'w') as f:
            json.dump(artifact.dict(), f)

        LOGGER.info(f'saved artifact_id={artifact_id} to {path_artifact}')

        dfs: list[pd.DataFrame] = []

        for query in artifact.dataset.queries:
            # LOAD
            LOGGER.info(f"EXECUTE -  LOAD {query.datasource_name}")

            ds = self.config.datasources.get(query.datasource_name)
            if not ds:
                raise ValueError()

            df_single_datasource: pd.DataFrame = ds.get()  # not yet implemented, but should return a pd df

            # SELECT
            LOGGER.info(f"EXECUTE -  SELECT {query.datasource_name}")

            selected_features: list[QueryFeature] = query.features
            selected_features_names: list[str] = [sf.feature_name for sf in selected_features]

            df_single_datasource_select = df_single_datasource[selected_features_names]

            # FILTER
            LOGGER.info(f"EXECUTE - FILTER {query.datasource_name}")

            df_filtered = df_single_datasource_select.copy()

            for query_filter in query.filters:

                feature_name: str = query_filter.feature.feature_name
                operation_on_feature: Operations = Operations[query_filter.operation]
                operation_on_feature_parameter: str = query_filter.parameter

                apply_filter = {
                    Operations.NUM_LESS_THAN: lambda df: df[df[feature_name] < float(operation_on_feature_parameter)],
                    Operations.NUM_LESS_EQUAL: lambda df: df[df[feature_name] <= float(operation_on_feature_parameter)],
                    Operations.NUM_GREATER_THAN: lambda df: df[df[feature_name] > float(operation_on_feature_parameter)],
                    Operations.NUM_GREATER_EQUAL: lambda df: df[df[feature_name] >= float(operation_on_feature_parameter)],
                    Operations.NUM_EQUALS: lambda df: df[df[feature_name] == float(operation_on_feature_parameter)],
                    Operations.NUM_NOT_EQUALS: lambda df: df[df[feature_name] != float(operation_on_feature_parameter)],

                    Operations.OBJ_LIKE: lambda df: df[df[feature_name] == operation_on_feature_parameter],
                    Operations.OBJ_NOT_LIKE: lambda df: df[df[feature_name] != operation_on_feature_parameter],

                    Operations.TIME_BEFORE: lambda df: df[df[feature_name] < pd.to_datetime(operation_on_feature_parameter)],
                    Operations.TIME_AFTER: lambda df: df[df[feature_name] > pd.to_datetime(operation_on_feature_parameter)],
                    Operations.TIME_EQUALS: lambda df: df[df[feature_name] == pd.to_datetime(operation_on_feature_parameter)],
                    Operations.TIME_NOT_EQUALS: lambda df: df[df[feature_name] != pd.to_datetime(operation_on_feature_parameter)],
                }

                df_filtered = apply_filter[operation_on_feature](df_filtered)

                LOGGER.info(f"Applying {operation_on_feature}({operation_on_feature_parameter}) on {feature_name}")

            # TRANSFORM
            LOGGER.info(f"EXECUTE -  TRANSFORM {query.datasource_id}")

            # TODO

            # TERMINATE
            LOGGER.info(f"EXECUTE -  Finished with datasource {query.datasource_id}")

            # TODO

            dfs.append(df_filtered)

        df_all_datasources = pd.concat(dfs)

        path_datasource = os.path.join(working_folder, f'{artifact.artifact_id}_data.csv.gz')

        df_all_datasources.to_csv(path_datasource, compression='gzip')

        LOGGER.info(f'saved artifact_id={artifact_id} data to {path_datasource}')

        # dataset preparation
        label = artifact.dataset.label
        val_p = artifact.dataset.val_percentage
        test_p = artifact.dataset.test_percentage

        if label is None:
            LOGGER.error('label is note defined!')
            # TODO: raise exception
            return

        X_tr = df_all_datasources.drop(label).values
        Y_tr = df_all_datasources[label].values

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
        if X_ts and Y_ts:
            metrics = local_model.eval(X_ts, Y_ts)
            metrics.source = 'test'
            metrics.artifact_id = artifact_id
            self.routes_service.post_metrics(metrics)

        # model validation
        if X_val and Y_val:
            metrics = local_model.eval(X_val, Y_val)
            metrics.source = 'val'
            metrics.artifact_id = artifact_id
            self.routes_service.post_metrics(metrics)

        self.routes_service.post_model(artifact_id, path_model)
