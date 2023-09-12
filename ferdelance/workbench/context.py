from typing import Any

from ferdelance import __version__
from ferdelance.logging import get_logger
from ferdelance.schemas.node import NodePublicKey
from ferdelance.shared.checksums import str_checksum
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
    WorkbenchProjectToken,
)
from ferdelance.shared.exchange import Exchange
from ferdelance.shared.status import ArtifactJobStatus

from time import sleep, time
from uuid import uuid4

import json
import pickle
import requests
import os

LOGGER = get_logger(__name__)

HOME = os.path.expanduser("~")
DATA_DIR = os.environ.get("DATA_HOME", os.path.join(HOME, ".local", "share", "ferdelance"))
CONFIG_DIR = os.environ.get("CONFIG_HOME", os.path.join(HOME, ".config", "ferdelance"))
CACHE_DIR = os.environ.get("CACHE_HOME", os.path.join(HOME, ".cache", "ferdelance"))


class Context:
    """Main point of contact between the workbench and the server."""

    def __init__(
        self,
        server: str,
        ssh_key_path: str | None = None,
        generate_keys: bool = True,
        name: str = "",
        id_path: str | None = None,
    ) -> None:
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

        if id_path is None:
            id_path = os.path.join(DATA_DIR, "id")

            if os.path.exists(ssh_key_path):
                with open(id_path, "r") as f:
                    self.id: str = f.read()
            else:
                self.id: str = str(uuid4())

                with open(id_path, "w") as f:
                    f.write(self.id)
        else:
            with open(id_path, "r") as f:
                self.id: str = f.read()

        # connecting to server
        headers = self.exc.create_header(False)

        response_key = requests.get(
            "/node/key",
            headers=headers,
        )

        response_key.raise_for_status()

        spk = NodePublicKey(**response_key.json())

        self.exc.set_remote_key(spk.public_key)

        public_key = self.exc.transfer_public_key()

        data_to_sign = f"{self.id}:{public_key}"

        checksum = str_checksum(data_to_sign)
        signature = self.exc.sign(data_to_sign)

        wjr = WorkbenchJoinRequest(
            id=self.id,
            public_key=self.exc.transfer_public_key(),
            version=__version__,
            name=name,
            checksum=checksum,
            signature=signature,
        )

        _, payload = self.exc.create_payload(wjr.json())
        headers = self.exc.create_header(True)

        res = requests.post(
            f"{self.server}/workbench/connect",
            headers=headers,
            data=payload,
        )

        res.raise_for_status()

    def project(self, token: str | None = None) -> Project:
        if token is None:
            token = os.environ.get("PROJECT", None)

        if token is None:
            token = os.environ.get("PROJECT_TOKEN", None)

        if token is None:
            raise ValueError("Project token not found")

        wpt = WorkbenchProjectToken(token=token)

        headers, payload = self.create(self.id, wpt.json())

        res = requests.get(
            f"{self.server}/workbench/project",
            headers=headers,
            data=payload,
        )

        res.raise_for_status()

        _, res_payload = self.exc.get_payload(res.content)

        data = Project(**json.loads(res_payload))

        return data

    def clients(self, project: Project) -> list[Client]:
        """List all clients available on the server.

        :raises HTTPError:
            If the return code of the response is not a 2xx type.
        :returns:
            A list of client ids.
        """
        wpt = WorkbenchProjectToken(token=project.token)

        headers, payload = self.create(self.id, wpt.json())

        res = requests.get(
            f"{self.server}/workbench/clients",
            headers=headers,
            data=payload,
        )

        res.raise_for_status()

        _, data = self.exc.get_payload(res.content)

        data = WorkbenchClientList(**json.loads(data))

        return data.clients

    def datasources(self, project: Project) -> list[DataSource]:
        """List all data sources available.

        :raises HTTPError:
            If the return code of the response is not a 2xx type.
        :returns:
            A list of all datasources available.
        """
        wpt = WorkbenchProjectToken(token=project.token)

        headers, payload = self.create(self.id, wpt.json())

        res = requests.get(
            f"{self.server}/workbench/datasources",
            headers=headers,
            data=payload,
        )

        res.raise_for_status()

        _, data = self.exc.get_payload(res.content)

        data = WorkbenchDataSourceIdList(**json.loads(data))

        return data.datasources

    def execute(
        self,
        project: Project,
        estimate: QueryEstimate,
        wait_interval: int = 1,
        max_time: int = 30,
    ) -> Any:
        """Execute a statistical query."""

        artifact = Artifact(
            project_id=project.id,
            transform=estimate.transform,
            estimator=estimate.estimator,
        )

        headers, payload = self.create(self.id, artifact.json())

        res = requests.post(
            f"{self.server}/workbench/artifact/submit",
            headers=headers,
            data=payload,
        )

        res.raise_for_status()

        _, data = self.exc.get_payload(res.content)

        art_status = ArtifactStatus(**json.loads(data))
        artifact.id = art_status.id

        start_time = time()
        while art_status.status not in (
            ArtifactJobStatus.ERROR.name,
            ArtifactJobStatus.COMPLETED.name,
        ):
            print(".", end="")
            sleep(wait_interval)

            art_status = self.status(art_status)

            if time() > start_time + max_time:
                raise ValueError("Timeout exceeded")

        if not art_status.id:
            raise ValueError("Invalid artifact status")

        estimate = self.get_result(artifact)

        if art_status.status == ArtifactJobStatus.COMPLETED.name:
            return estimate

        if art_status.status == ArtifactJobStatus.ERROR.name:
            LOGGER.error(f"Error on artifact {art_status.id}")
            LOGGER.error(estimate)
            raise ValueError(f"Error on artifact {art_status.id}")

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
            project_id=project.id,
            transform=query.transform,
            plan=query.plan,
            model=query.model,
        )

        headers, payload = self.create(self.id, artifact.json())

        res = requests.post(
            f"{self.server}/workbench/artifact/submit",
            headers=headers,
            data=payload,
        )

        res.raise_for_status()

        _, data = self.exc.get_payload(res.content)

        status = ArtifactStatus(**json.loads(data))
        artifact.id = status.id

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
        if not artifact.id:
            raise ValueError("submit first the artifact to the server")

        wba = WorkbenchArtifact(artifact_id=artifact.id)

        headers, payload = self.create(self.id, wba.json())

        res = requests.get(
            f"{self.server}/workbench/artifact/status",
            headers=headers,
            data=payload,
        )

        res.raise_for_status()

        _, data = self.exc.get_payload(res.content)

        return ArtifactStatus(**json.loads(data))

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

        headers, payload = self.create(self.id, wba.json())

        res = requests.get(
            f"{self.server}/workbench/artifact",
            headers=headers,
            data=payload,
        )

        res.raise_for_status()

        _, data = self.exc.get_payload(res.content)

        return Artifact(**json.loads(data))

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
        if not artifact.id:
            raise ValueError("submit first the artifact to the server")

        wba = WorkbenchArtifact(artifact_id=artifact.id)

        headers, payload = self.create(self.id, wba.json())

        with requests.get(
            f"{self.server}/workbench/result",
            headers=headers,
            data=payload,
            stream=True,
        ) as res:
            res.raise_for_status()

            data, _ = self.exc.stream_response(res.iter_content())

            obj = pickle.loads(data)

            return obj

    def get_partial_result(self, artifact: Artifact, client_id: str) -> Any:
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
        if not artifact.id:
            raise ValueError("submit first the artifact to the server")

        if artifact.model is None:
            raise ValueError("no model associated with this artifact")

        headers, _ = self.create(self.id, "")

        with requests.get(
            f"{self.server}/workbench/result/partial/{artifact.id}/{client_id}",
            headers=headers,
            stream=True,
        ) as res:
            res.raise_for_status()

            data, _ = self.exc.stream_response(res.iter_content())

            obj = pickle.loads(data)

            return obj
