import json
from pathlib import Path
from ferdelance.client.services.routes import RouteService
from ferdelance_shared.schemas import ArtifactTask, UpdateExecute


import logging

LOGGER = logging.getLogger(__name__)

class ExecuteAction:
    def __init__(self, routes_service: RouteService, update_execute: UpdateExecute) -> None:
        self.routes_service = routes_service
        self.update_execute = update_execute

    def execute(self, ) -> None:
        LOGGER.info('executing new task')
        content: ArtifactTask = self.routes_service.get_task(self.update_execute)

        # TODO: this is an example, execute required task when implemented

        LOGGER.info(f'received artifact_id={content.artifact_id}')

        with open(Path(self.config.path_artifact_folder) / Path(f'{content.artifact_id}.json'), 'w') as f:
            json.dump(content.dict(), f)