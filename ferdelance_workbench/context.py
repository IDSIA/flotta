from ferdelance_workbench.exceptions import ServerError
from ferdelance_workbench.artifacts import *
from ferdelance_workbench.models import *

from uuid import uuid4

import json
import logging
import requests

LOGGER = logging.getLogger(__name__)


class Context:

    def __init__(self, server: str) -> None:
        self.server = server.rstrip('/')

    def list_clients(self) -> list[str]:
        """List all clients available on the server.

        :raises HTTPError:
            If the return code of the response is not a 2xx type.
        :returns:
            A list of client ids.
        """
        res = requests.get(f'{self.server}/workbench/client/list')

        res.raise_for_status()

        return res.json()

    def detail_client(self, client_id: str) -> ClientDetails:
        """List the details of a client.

        :param client_id: 
            This is one of the ids returned with the `list_clients()` method.
        :raises HTTPError:
            If the return code of the response is not a 2xx type.
        :returns:
            The details for the given client.
        """
        res = requests.get(f'{self.server}/workbench/client/{client_id}')

        if res.status_code != 200:
            raise ServerError(f'server status code: {res.status_code}')

        return ClientDetails(**res.json())

    def list_datasources(self) -> list[str]:
        """List all data sources available.

        :raises HTTPError:
            If the return code of the response is not a 2xx type.
        :returns:
            A list of all datasources available.
        """
        res = requests.get(f'{self.server}/workbench/datasource/list/')

        if res.status_code != 200:
            raise ServerError(f'server status code: {res.status_code}')

        return res.json()

    def detail_datasource(self, datasource_id: str) -> DataSource:
        """Returns the detail, like metadata, of the given datasource.

        :param datasource_id:
            This is one of the ids returned with the `list_datasources()` method.
        :raises HTTPError:
            If the return code of the response is not a 2xx type.
        :returns:
            The details for the given datasource, with also features.
        """
        res = requests.get(f'{self.server}/workbench/datasource/{datasource_id}')

        if res.status_code != 200:
            raise ServerError(f'server status code: {res.status_code}')

        return DataSource(**res.json())

    def submit(self, artifact: Artifact, ret_status: bool = False) -> Artifact | tuple[Artifact, ArtifactStatus]:
        """Submit the query, model, and strategy and start a training task on the remote server.

        :param artifact:
            Artifact to submit to the server for training.
        :param ret_status:
            If true, this method returns also the status from the server.
        :raises HTTPError:
            If the return code of the response is not a 2xx type.
        :returns:
            The same input artifact with an assigned artifact_id.
            If the `ret_status` flag is true, the status of the artifact is also returned.
        """
        res = requests.post(f'{self.server}/workbench/artifact/submit', json=artifact.dict())

        res.raise_for_status()

        status = ArtifactStatus(**res.json())
        artifact.artifact_id = status.artifact_id

        if ret_status:
            return artifact, status

        return artifact

    def status(self, artifact: Artifact | ArtifactStatus) -> ArtifactStatus:
        """Poll the server to get an update of the status of the given artifact.

        :param artifact:
            Artifact or ArtifactStatus to get an update for.
        :raises HTTPError:
            If the return code of the response is not a 2xx type.
        :returns:
            An ArtifactStatus object with the status of the artifact on the server.
        """
        if artifact.artifact_id is None:
            raise ValueError('submit first the artifact to the server')

        res = requests.get(f'{self.server}/workbench/artifact/{artifact.artifact_id}')

        res.raise_for_status()

        return ArtifactStatus(**res.json())

    def get_artifact(self, artifact_id: str) -> Artifact:
        """Get the specified artifact from the server.

        :param artifact_id:
            Artifact to get the model from.
        :raises HTTPError:
            If the return code of the response is not a 2xx type.
        :returns:
            The artifact saved on the server and associated with the given artifact_id.
        """
        res = requests.get(f'{self.server}/workbench/download/artifact/{artifact_id}')

        res.raise_for_status()

        return Artifact(**json.loads(res.content))

    def get_model(self, artifact: Artifact, path: str = None) -> Model:
        """Get the trained and aggregated model from the artifact and save it to disk.

        :param artifact:
            Artifact to get the model from.
        :param path:
            Optional, destination path on disk. If none, a UUID will be used to store the downloaded model.
        :raises HTTPError:
            If the return code of the response is not a 2xx type.
        """
        if artifact.artifact_id is None:
            raise ValueError('submit first the artifact to the server')

        res = requests.get(f'{self.server}/workbench/download/model/{artifact.artifact_id}')

        res.raise_for_status()

        if path is None:
            path = f'{artifact.model.name}.{uuid4()}.model.bin'

        with open(path, 'wb') as f:
            f.write(res.content)

        m = artifact.model
        m.load(path)
        return m
