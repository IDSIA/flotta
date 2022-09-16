from typing import Any
import requests
import logging

from ferdelance_workbench.artifacts import Artifact, Query, Model, Strategy
from ferdelance_workbench.exceptions import ServerError
from ferdelance_workbench.schema.workbench import *

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

    def list_datasources(self) -> list[dict]:
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

    def create_artifact(self) -> Artifact:
        """Initialize a new artifacts.

        :raises HTTPError: if the return code of the server is not 2xx
        """

        return Artifact()

    def submit(self, query: Query, model: Model, strategy: Strategy) -> Artifact:
        return Artifact()

    def status(self, artifact: Artifact) -> None:
        pass
