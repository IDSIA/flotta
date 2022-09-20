from typing import Any

from ferdelance_workbench.artifacts import Artifact, Query, Model, Strategy, artifact_from_json
from ferdelance_workbench.exceptions import ServerError
from ferdelance_workbench.schema.workbench import *

import json
import logging
import requests

LOGGER = logging.getLogger(__name__)


class Context:

    def __init__(self, server: str) -> None:
        self.server = server.rstrip('/')

    def list_clients(self) -> list[str]:
        """List all clients available on the server.

        :raises HTTPError: if the return code of the server is not 2xx
        """
        res = requests.get(f'{self.server}/workbench/client/list')

        res.raise_for_status()

        return res.json()

    def detail_client(self, client_id: str) -> ClientDetails:
        """List the details of a client.

        :param client_id: 
            This is one of the ids returned with the `list_clients()` method.

        :raises HTTPError: if the return code of the server is not 2xx
        """
        res = requests.get(f'{self.server}/workbench/client/{client_id}')

        if res.status_code != 200:
            raise ServerError(f'server status code: {res.status_code}')

        return ClientDetails(**res.json())

    def list_datasources(self) -> list[int]:
        """List all data sources available.

        :raises HTTPError: if the return code of the server is not 2xx
        """
        res = requests.get(f'{self.server}/workbench/datasource/list/')

        if res.status_code != 200:
            raise ServerError(f'server status code: {res.status_code}')

        return res.json()

    def detail_datasource(self, datasource_id: int) -> DataSourceDetails:
        """Returns the detail, like metadata, of the given datasource.

        :param datasource_id:
            This is one of the ids returned with the `list_datasources()` method.

        :raises HTTPError: if the return code of the server is not 2xx
        """
        res = requests.get(f'{self.server}/workbench/datasource/{datasource_id}')

        if res.status_code != 200:
            raise ServerError(f'server status code: {res.status_code}')

        return DataSourceDetails(**res.json())

    def submit(self, query: Query, model: Model, strategy: Strategy) -> Artifact:
        """Submit the query, model, and strategy and start a training task on the remote server.

        :param query:
            The query to use to get the training data.
        :param model:
            The model to use for training.
        :param strategy:
            The strategy to use to aggregate the models.
        """
        res = requests.post(f'{self.server}/workbench/artifact/submit', json={
            'query': query.json(),
            'model': model.json(),
            'strategy': strategy.json(),
        })

        res.raise_for_status()

        data = res.json()

        return Artifact(**data)

    def status(self, artifact: Artifact) -> Artifact:
        """Poll the server to get an update of the status of the given artifact.

        :param artifact:
            Artifact to get an update for.
        """
        if artifact.artifact_id is None:
            raise ValueError('submit first the artifact to the server')

        res = requests.get(f'{self.server}/workbench/artifact/{artifact.artifact_id}')

        res.raise_for_status()

        data = res.json()

        artifact.update(data)
        return artifact

    def get_artifact(self, artifact_id: str) -> Artifact:
        """Get the trained and aggregated model from the artifact and save it to disk.

        :param artifact:
            Artifact to get the model from.
        """
        res = requests.get(f'{self.server}/workbench/download/artifact/{artifact_id}')

        res.raise_for_status()

        data = json.loads(res.content)

        return artifact_from_json(artifact_id, data)

    def get_model(self, artifact: Artifact, path: str) -> Any:
        """Get the trained and aggregated model from the artifact and save it to disk.

        :param artifact:
            Artifact to get the model from.
        :param path:
            Destination path on disk.
        """
        if artifact.artifact_id is None:
            raise ValueError('submit first the artifact to the server')

        res = requests.get(f'{self.server}/workbench/download/model/{artifact.artifact_id}')

        res.raise_for_status()

        with open(path, 'wb') as f:
            f.write(res.content)
