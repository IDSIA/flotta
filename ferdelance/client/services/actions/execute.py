from ferdelance_shared.schemas import Artifact, UpdateExecute, QueryFeature
from ferdelance_shared.operations import NumericOperations, ObjectOperations, TimeOperations
from ferdelance.client.config import Config
from ferdelance.client.services.routes import RouteService
from .action import Action

from pathlib import Path

import pandas as pd

import json
import logging

LOGGER = logging.getLogger(__name__)


class ExecuteAction(Action):

    def __init__(self, config: Config, update_execute: UpdateExecute) -> None:
        self.config = config
        self.routes_service: RouteService = RouteService(config)
        self.update_execute = update_execute

    def validate_input(self) -> None:
        ...

    def execute(self) -> None:
        LOGGER.info('executing new task')

        artifact: Artifact = self.routes_service.get_task(self.update_execute)

        dfs: list[pd.DataFrame] = []

        for query in artifact.dataset.queries:
            #
            # LOAD
            #

            LOGGER.info(f"EXECUTE -  LOAD {query.datasource_name}")

            ds = self.config.datasources.get(query.datasource_name)
            if not ds:
                raise ValueError()

            df_single_datasource: pd.DataFrame = ds.get()  # not yet implemented, but should return a pd df

            #
            # SELECT
            #

            LOGGER.info(f"EXECUTE -  SELECT {query.datasource_name}")

            selected_features: list[QueryFeature] = query.features
            selected_features_names: list[str] = [sf.feature_name for sf in selected_features]

            df_single_datasource_select = df_single_datasource[selected_features_names]

            #
            # FILTER
            #

            LOGGER.info(f"EXECUTE - FILTER {query.datasource_name}")

            df_filtered = df_single_datasource_select.copy()

            for query_filter in query.filters:

                feature_name: str = query_filter.feature.feature_name
                operation_on_feature: str = query_filter.operation
                operation_on_feature_parameter: str = query_filter.parameter

                apply_filter = {
                    NumericOperations.LESS_THAN: lambda df: df[df[feature_name] < float(operation_on_feature_parameter)],
                    NumericOperations.LESS_EQUAL: lambda df: df[df[feature_name] <= float(operation_on_feature_parameter)],
                    NumericOperations.GREATER_THAN: lambda df: df[df[feature_name] > float(operation_on_feature_parameter)],
                    NumericOperations.GREATER_EQUAL: lambda df: df[df[feature_name] >= float(operation_on_feature_parameter)],
                    NumericOperations.EQUALS: lambda df: df[df[feature_name] == float(operation_on_feature_parameter)],
                    NumericOperations.NOT_EQUALS: lambda df: df[df[feature_name] != float(operation_on_feature_parameter)],

                    ObjectOperations.LIKE: lambda df: df[df[feature_name] == operation_on_feature_parameter],
                    ObjectOperations.NOT_LIKE: lambda df: df[df[feature_name] != operation_on_feature_parameter],

                    TimeOperations.BEFORE: lambda df: df[df[feature_name] < pd.to_datetime(operation_on_feature_parameter)],
                    TimeOperations.AFTER: lambda df: df[df[feature_name] > pd.to_datetime(operation_on_feature_parameter)],
                    TimeOperations.EQUALS: lambda df: df[df[feature_name] == pd.to_datetime(operation_on_feature_parameter)],
                    TimeOperations.NOT_EQUALS: lambda df: df[df[feature_name] != pd.to_datetime(operation_on_feature_parameter)],
                }

                df_filtered = apply_filter[operation_on_feature](df_filtered)

                LOGGER.info(f"Applying {operation_on_feature}({operation_on_feature_parameter}) on {feature_name}")

            #
            # TRANSFORM
            #
            LOGGER.info(f"EXECUTE -  TRANSFORM {query.datasource_id}")

            #
            # TERMINATE
            #
            LOGGER.info(f"EXECUTE -  Finished with datasource {query.datasource_id}")

            dfs.append(df_filtered)

        df_all_datasources = pd.concat(dfs)

        # TODO: this is an example, execute required task when implemented

        LOGGER.info(f'received artifact_id={artifact.artifact_id}')

        with open(Path(self.config.path_artifact_folder) / Path(f'{artifact.artifact_id}.json'), 'w') as f:
            json.dump(artifact.dict(), f)
