import json
from pathlib import Path
from typing import Callable, List
import pandas as pd
from ferdelance.client.config import Config
from ferdelance.client.services.routes import RouteService
from ferdelance_shared.schemas import Artifact, DataSource, Feature, UpdateExecute
from ferdelance_shared.operations import NumericOperations, ObjectOperations, TimeOperations


import logging

LOGGER = logging.getLogger(__name__)

class ExecuteAction:
    def __init__(self, config: Config, routes_service: RouteService, update_execute: UpdateExecute) -> None:
        self.config = config
        self.routes_service = routes_service
        self.update_execute = update_execute

    def execute(self, ) -> None:
        LOGGER.info('executing new task')

        artifact: Artifact = self.routes_service.get_task(self.update_execute)

        dfs: List[pd.DataFrame] = []

        for query in artifact.dataset.queries:
            #
            # LOAD
            #

            LOGGER.info(f"EXECUTE -  LOAD {query.datasources_name}")


            df_single_datasource: pd.DataFrame = self.config.datasources.get(query.datasources_name).get() # not yet implemented, but should return a pd df
            
            #
            # SELECT
            #

            LOGGER.info(f"EXECUTE -  SELECT {query.datasources_name}")

            selected_features: List[Feature] = query.features
            selected_features_names: List[str] = [sf.feature_name for sf in selected_features]

            df_single_datasource_select = df_single_datasource[selected_features_names]

            #
            # FILTER
            #

            LOGGER.info(f"EXECUTE - FILTER {query.datasources_name}")

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
            LOGGER.info(f"EXECUTE -  TRANSFORM {query.datasources_id}")
            
            #
            # TERMINATE
            #
            LOGGER.info(f"EXECUTE -  Finished with datasource {query.datasources_id}")


            dfs.append(df_filtered)
        
        df_all_datasources = pd.concat(dfs)

       


        # TODO: this is an example, execute required task when implemented

        LOGGER.info(f'received artifact_id={artifact.artifact_id}')

        with open(Path(self.config.path_artifact_folder) / Path(f'{artifact.artifact_id}.json'), 'w') as f:
            json.dump(artifact.dict(), f)