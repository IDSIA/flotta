import json
from pathlib import Path
from typing import Callable, List
import pandas as pd
from ferdelance.client.config import Config
from ferdelance.client.services.routes import RouteService
from ferdelance_shared.schemas import ArtifactTask, DataSource, Feature, UpdateExecute
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
        content: ArtifactTask = self.routes_service.get_task(self.update_execute)

        dfs: List[pd.DataFrame] = []
        for query in content.dataset.queries:

            #
            # LOAD
            #
            LOGGER.info(f"EXECUTE -  LOAD {query.datasources_id}")


            LOGGER.info("===========================")
            LOGGER.info(self.config.datasources_by_id)
            LOGGER.info(query)
            LOGGER.info("===========================")


            df_single_datasource: pd.DataFrame = self.config.datasources_by_id.get(query.datasources_id).get() # not yet implemented, but should return a pd df
            
            # #
            # # SELECT
            # #

            # LOGGER.info(f"EXECUTE -  SELECT {query.datasources_id}")

            # select_metadata = lambda metadata: metadata.datasource_id == query.datasources_id
            # select_features = lambda feature: feature.feature_id in [f.feature_id for f in query.features]

            # single_datasource_metadata: DataSource = list(filter(select_metadata, datasources_metadata_list))[0]
            # selected_features: List[Feature] = list(filter(select_features, single_datasource_metadata.features))
            # selected_features_names: List[str] = [f.name for f in selected_features]

            # df_single_datasource_select = df_single_datasource[selected_features_names]

            # #
            # # FILTER
            # #
            # LOGGER.info(f"EXECUTE - FILTER {query.datasources_id}")


            # df_filtered = df_single_datasource_select.copy()

            # for filter in query.filters:
            #     select_single_feature = lambda feature: feature.feature_id == filter.feature.feature_id

            #     feature_name = list(filter(select_single_feature, single_datasource_metadata.features))
            #     operation_on_feature: str = filter.operation
            #     operation_on_feature_parameter: str = filter.parameter

            #     apply_filter = {
            #         NumericOperations.LESS_THAN: lambda df: df[df[feature_name] < float(operation_on_feature_parameter)],
            #         NumericOperations.LESS_EQUAL: lambda df: df[df[feature_name] <= float(operation_on_feature_parameter)],
            #         NumericOperations.GREATER_THAN: lambda df: df[df[feature_name] > float(operation_on_feature_parameter)],
            #         NumericOperations.GREATER_EQUAL: lambda df: df[df[feature_name] >= float(operation_on_feature_parameter)],
            #         NumericOperations.EQUALS: lambda df: df[df[feature_name] == float(operation_on_feature_parameter)],
            #         NumericOperations.NOT_EQUALS: lambda df: df[df[feature_name] != float(operation_on_feature_parameter)],
                    
            #         ObjectOperations.LIKE: lambda df: df[df[feature_name] == operation_on_feature_parameter],
            #         ObjectOperations.NOT_LIKE: lambda df: df[df[feature_name] != operation_on_feature_parameter],

            #         TimeOperations.BEFORE: lambda df: df[df[feature_name] < pd.to_datetime(operation_on_feature_parameter)],
            #         TimeOperations.AFTER: lambda df: df[df[feature_name] > pd.to_datetime(operation_on_feature_parameter)],
            #         TimeOperations.EQUALS: lambda df: df[df[feature_name] == pd.to_datetime(operation_on_feature_parameter)],
            #         TimeOperations.NOT_EQUALS: lambda df: df[df[feature_name] != pd.to_datetime(operation_on_feature_parameter)],
            #     }

            #     df_filtered = apply_filter[operation_on_feature](df_filtered)

            #     LOGGER.info(f"Applying {operation_on_feature}({operation_on_feature_parameter}) on {feature_name}")

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

        LOGGER.info(f'received artifact_id={content.artifact_id}')

        with open(Path(self.config.path_artifact_folder) / Path(f'{content.artifact_id}.json'), 'w') as f:
            json.dump(content.dict(), f)