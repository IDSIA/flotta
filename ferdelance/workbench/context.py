from typing import Any
from ferdelance.workbench.interface import (
    Project,
    Client,
    DataSource,
    Artifact,
    ArtifactStatus,
)
from ferdelance.schemas.queries import QueryModel, QueryEstimate
from ferdelance.schemas.workbench import (
    WorkbenchArtifact,
    WorkbenchClientList,
    WorkbenchDataSourceIdList,
    WorkbenchJoinRequest,
    WorkbenchJoinData,
    WorkbenchProjectToken,
)
from ferdelance.shared.exchange import Exchange
from ferdelance.shared.status import ArtifactJobStatus

from time import sleep

import json
import logging
import pickle
import requests
import os

LOGGER = logging.getLogger(__name__)

HOME = os.path.expanduser("~")
DATA_DIR = os.environ.get("DATA_HOME", os.path.join(HOME, ".local", "share", "ferdelance"))
CONFIG_DIR = os.environ.get("CONFIG_HOME", os.path.join(HOME, ".config", "ferdelance"))
CACHE_DIR = os.environ.get("CACHE_HOME", os.path.join(HOME, ".cache", "ferdelance"))


class Context:
    """Main point of contact between the workbench and the server."""

    def __init__(self, server: str, ssh_key_path: str | None = None, generate_keys: bool = True) -> None:
        """Connect to the given server, and establish all the requirements for a secure interaction.

        :param server:
            URL of the server to connect to.
        :param ssh_key:
            Path to an existing SSH private keys on local disk.
        :param generate_keys:
            If True and `ssh_key` is None, then a new SSH key will be generated locally. Otherwise
            if False, the key stored in `HOME/.ssh/rsa_id` will be used.
        """
        self.server: str = server.rstrip("/")

        self.exc: Exchange = Exchange()

        os.makedirs(DATA_DIR, exist_ok=True)
        os.makedirs(CONFIG_DIR, exist_ok=True)
        os.makedirs(CACHE_DIR, exist_ok=True)

        if ssh_key_path is None:
            if generate_keys:
                ssh_key_path = os.path.join(DATA_DIR, "rsa_id")

                if os.path.exists(ssh_key_path):
                    LOGGER.debug(f"loading private key from {ssh_key_path}")

                    self.exc.load_key(ssh_key_path)

                else:
                    LOGGER.debug(f"generating and saving private key to {ssh_key_path}")

                    self.exc.generate_key()
                    self.exc.save_private_key(ssh_key_path)

            else:
                ssh_key_path = os.path.join(HOME, ".ssh", "rsa_id")

                LOGGER.debug(f"loading private key from {ssh_key_path}")

                self.exc.load_key(ssh_key_path)

        else:
            LOGGER.debug(f"loading private key from {ssh_key_path}")

            self.exc.load_key(ssh_key_path)

        wjr = WorkbenchJoinRequest(public_key=self.exc.transfer_public_key())

        res = requests.post(f"{self.server}/workbench/connect", data=json.dumps(wjr.dict()))

        res.raise_for_status()

        data = WorkbenchJoinData(**self.exc.get_payload(res.content))

        self.workbench_id: str = data.id

        self.exc.set_token(data.token)
        self.exc.set_remote_key(data.public_key)

    def load(self, token: str) -> Project:
        wpt = WorkbenchProjectToken(token=token)

        res = requests.get(
            f"{self.server}/workbench/project",
            headers=self.exc.headers(),
            data=self.exc.create_payload(wpt.dict()),
        )

        res.raise_for_status()

        data = Project(**self.exc.get_payload(res.content))

        return data

    def clients(self, project: Project) -> list[Client]:
        """List all clients available on the server.

        :raises HTTPError:
            If the return code of the response is not a 2xx type.
        :returns:
            A list of client ids.
        """
        wpt = WorkbenchProjectToken(token=project.token)

        res = requests.get(
            f"{self.server}/workbench/clients",
            headers=self.exc.headers(),
            data=self.exc.create_payload(wpt.dict()),
        )

        res.raise_for_status()

        data = WorkbenchClientList(**self.exc.get_payload(res.content))

        return data.clients

    def datasources(self, project: Project) -> list[DataSource]:
        """List all data sources available.

        :raises HTTPError:
            If the return code of the response is not a 2xx type.
        :returns:
            A list of all datasources available.
        """
        wpt = WorkbenchProjectToken(token=project.token)

        res = requests.get(
            f"{self.server}/workbench/datasources",
            headers=self.exc.headers(),
            data=self.exc.create_payload(wpt.dict()),
        )

        res.raise_for_status()

        data = WorkbenchDataSourceIdList(**self.exc.get_payload(res.content))

        return data.datasources

    def execute(self, project: Project, estimate: QueryEstimate, path: str = "", wait_interval: int = 1) -> Any:
        """Execute a statistical query."""

        artifact = Artifact(
            project_id=project.project_id,
            transform=estimate.transform,
            estimate=estimate.estimator,
        )

        res = requests.post(
            f"{self.server}/workbench/artifact/submit",
            headers=self.exc.headers(),
            data=self.exc.create_payload(artifact.dict()),
        )

        res.raise_for_status()

        art_status = ArtifactStatus(**self.exc.get_payload(res.content))
        artifact.artifact_id = art_status.artifact_id

        while art_status.status not in (
            ArtifactJobStatus.ERROR.name,
            ArtifactJobStatus.COMPLETED.name,
        ):
            print(".", end="")
            sleep(wait_interval)

            art_status = self.status(art_status)

        if art_status.artifact_id is None:
            raise ValueError("Invalid artifact status")

        estimate = self.get_result(artifact)

        if art_status.status == ArtifactJobStatus.COMPLETED.name:
            return estimate

        if art_status.status == ArtifactJobStatus.ERROR.name:
            LOGGER.error(f"Error on artifact {art_status.artifact_id}")
            LOGGER.error(estimate)
            raise ValueError(f"Error on artifact {art_status.artifact_id}")

        raise NotImplementedError()

    def submit(self, project: Project, query: QueryModel) -> Artifact:
        """Submit the query, model, and strategy and start a training task on the remote server.

        :param artifact:
            Artifact to submit to the server for training.
            This object will be updated with the id assigned by the server.
        :raises HTTPError:
            If the return code of the response is not a 2xx type.
        :returns:
            The same input artifact with an assigned artifact_id.
            If the `ret_status` flag is true, the status of the artifact is also returned.
        """
        artifact: Artifact = Artifact(
            project_id=project.project_id,
            transform=query.transform,
            load=query.plan,
            model=query.model,
        )

        res = requests.post(
            f"{self.server}/workbench/artifact/submit",
            headers=self.exc.headers(),
            data=self.exc.create_payload(artifact.dict()),
        )

        res.raise_for_status()

        status = ArtifactStatus(**self.exc.get_payload(res.content))
        artifact.artifact_id = status.artifact_id

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
            raise ValueError("submit first the artifact to the server")

        wba = WorkbenchArtifact(artifact_id=artifact.artifact_id)

        res = requests.get(
            f"{self.server}/workbench/artifact/status",
            headers=self.exc.headers(),
            data=self.exc.create_payload(wba.dict()),
        )

        res.raise_for_status()

        return ArtifactStatus(**self.exc.get_payload(res.content))

    def get_artifact(self, artifact_id: str) -> Artifact:
        """Get the specified artifact from the server.

        :param artifact_id:
            Artifact to get the model from.
        :raises HTTPError:
            If the return code of the response is not a 2xx type.
        :returns:
            The artifact saved on the server and associated with the given artifact_id.
        """

        wba = WorkbenchArtifact(artifact_id=artifact_id)

        res = requests.get(
            f"{self.server}/workbench/artifact",
            headers=self.exc.headers(),
            data=self.exc.create_payload(wba.dict()),
        )

        res.raise_for_status()

        return Artifact(**self.exc.get_payload(res.content))

    def get_result(self, artifact: Artifact) -> Any:
        """Get the trained and aggregated result for the the artifact and save it
        to disk. The result can be a model or an estimation.

        :param artifact:
            Artifact to get the result from.
        :param path:
            Optional, destination path on disk. If none, a UUID will be generated
            and used to store the downloaded model.
        :raises HTTPError:
            If the return code of the response is not a 2xx type.
        """
        if artifact.artifact_id is None:
            raise ValueError("submit first the artifact to the server")

        wba = WorkbenchArtifact(artifact_id=artifact.artifact_id)

        with requests.get(
            f"{self.server}/workbench/result",
            headers=self.exc.headers(),
            data=self.exc.create_payload(wba.dict()),
            stream=True,
        ) as res:

            res.raise_for_status()

            data, _ = self.exc.stream_response(res)

            obj = pickle.loads(data)

            return obj

    def get_partial_result(self, artifact: Artifact, client_id: str, path: str = "") -> Any:
        """Get the trained partial model from the artifact and save it to disk.

        :param artifact:
            Artifact to get the model from.
        :param client_id:
            The client_it that trained the partial model.
        :param path:
            Optional, destination path on disk. If none, a UUID will be used to store the downloaded model.
        :raises HTTPError:
            If the return code of the response is not a 2xx type.
        """
        if artifact.artifact_id is None:
            raise ValueError("submit first the artifact to the server")

        if artifact.model is None:
            raise ValueError("no model associated with this artifact")

        if not path:
            path = f"{artifact.artifact_id}.{artifact.model.name}.{client_id}.PARTIAL.model"

        with requests.get(
            f"{self.server}/workbench/result/partial/{artifact.artifact_id}/{client_id}",
            headers=self.exc.headers(),
            stream=True,
        ) as res:
            res.raise_for_status()

            data, _ = self.exc.stream_response(res, path)

            obj = pickle.loads(data)

            return obj
