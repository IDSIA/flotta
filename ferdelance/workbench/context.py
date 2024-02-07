from typing import Any, Sequence

from ferdelance import __version__
from ferdelance.core.interfaces import Step
from ferdelance.logging import get_logger
from ferdelance.schemas.node import NodePublicKey
from ferdelance.schemas.workbench import (
    WorkbenchArtifact,
    WorkbenchClientList,
    WorkbenchDataSourceIdList,
    WorkbenchJoinRequest,
    WorkbenchJoinResponse,
    WorkbenchProjectToken,
    WorkbenchResource,
)
from ferdelance.security.checksums import str_checksum
from ferdelance.security.exchange import Exchange
from ferdelance.shared.status import ArtifactJobStatus
from ferdelance.workbench.interface import (
    Project,
    Client,
    DataSource,
    Artifact,
    ArtifactStatus,
)

from pathlib import Path
from uuid import uuid4

import httpx
import json
import os
import pickle
import time

LOGGER = get_logger(__name__)

HOME: Path = Path(os.path.expanduser("~"))
DATA_DIR: Path = Path(os.environ.get("DATA_HOME", str(HOME / ".local" / "share" / "ferdelance")))
CONFIG_DIR: Path = Path(os.environ.get("CONFIG_HOME", str(HOME / ".config" / "ferdelance")))
CACHE_DIR: Path = Path(os.environ.get("CACHE_HOME", str(HOME / ".cache" / "ferdelance")))


class Context:
    """Main point of contact between the workbench and the server."""

    def __init__(
        self,
        server: str,
        ssh_key_path: Path | str | None = None,
        generate_keys: bool = True,
        name: str = "",
        id_path: Path | str | None = None,
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
        self.server_url: str = server.rstrip("/")

        self.exc: Exchange

        os.makedirs(DATA_DIR, exist_ok=True)
        os.makedirs(CONFIG_DIR, exist_ok=True)
        os.makedirs(CACHE_DIR, exist_ok=True)

        if isinstance(id_path, str):
            if id_path == "":
                id_path = None
            else:
                id_path = Path(id_path)

        if id_path is None:
            id_path = DATA_DIR / "id"

            if os.path.exists(id_path):
                with open(id_path, "r") as f:
                    self.id: str = f.read()
            else:
                self.id: str = str(uuid4())

                with open(id_path, "w") as f:
                    f.write(self.id)
        else:
            with open(id_path, "r") as f:
                self.id: str = f.read()

        if isinstance(ssh_key_path, str):
            if ssh_key_path == "":
                ssh_key_path = None
            else:
                ssh_key_path = Path(ssh_key_path)

        if ssh_key_path is None:
            if generate_keys:
                ssh_key_path = DATA_DIR / "rsa_id"

                if os.path.exists(ssh_key_path):
                    LOGGER.debug(f"loading private key from {ssh_key_path}")

                    self.exc = Exchange(self.id, private_key_path=ssh_key_path)

                else:
                    LOGGER.debug(f"generating and saving private key to {ssh_key_path}")

                    self.exc = Exchange(self.id)
                    self.exc.store_private_key(ssh_key_path)

            else:
                ssh_key_path = HOME / ".ssh" / "rsa_id"

                if not os.path.exists(ssh_key_path):
                    raise ValueError(f"Key not found at {ssh_key_path}, please set flag for generate a new one.")

                LOGGER.debug(f"loading private key from {ssh_key_path}")

                self.exc = Exchange(self.id, private_key_path=ssh_key_path)

        else:
            LOGGER.debug(f"loading private key from {ssh_key_path}")

            self.exc = Exchange(self.id, private_key_path=ssh_key_path)

        # connecting to server
        response_key = httpx.get(
            f"{self.server_url}/node/key",
        )

        response_key.raise_for_status()

        spk = NodePublicKey(**response_key.json())

        self.exc.set_remote_key("JOIN", spk.public_key)

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

        headers, payload = self.exc.create(wjr.json())

        res = httpx.post(
            f"{self.server_url}/workbench/connect",
            headers=headers,
            content=payload,
        )

        res.raise_for_status()

        _, payload = self.exc.get_payload(res.content)

        join_data = WorkbenchJoinResponse(**(json.loads(payload)))

        self.exc.set_remote_key(join_data.component_id, spk.public_key)

        self.server_id = join_data.component_id

    def project(self, token: str | None = None) -> Project:
        if token is None:
            token = os.environ.get("PROJECT", None)

        if token is None:
            token = os.environ.get("PROJECT_TOKEN", None)

        if token is None:
            raise ValueError("Project token not found")

        wpt = WorkbenchProjectToken(token=token)

        headers, payload = self.exc.create(wpt.json())

        res = httpx.request(
            "GET",
            f"{self.server_url}/workbench/project",
            headers=headers,
            content=payload,
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

        headers, payload = self.exc.create(wpt.json())

        res = httpx.request(
            "GET",
            f"{self.server_url}/workbench/clients",
            headers=headers,
            content=payload,
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

        headers, payload = self.exc.create(wpt.json())

        res = httpx.request(
            "GET",
            f"{self.server_url}/workbench/datasources",
            headers=headers,
            content=payload,
        )

        res.raise_for_status()

        _, data = self.exc.get_payload(res.content)

        data = WorkbenchDataSourceIdList(**json.loads(data))

        return data.datasources

    def submit(self, project: Project, steps: Sequence[Step]) -> Artifact:
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
            steps=steps,
        )

        headers, payload = self.exc.create(artifact.json())

        res = httpx.post(
            f"{self.server_url}/workbench/artifact/submit",
            headers=headers,
            content=payload,
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

        headers, payload = self.exc.create(wba.json())

        res = httpx.request(
            "GET",
            f"{self.server_url}/workbench/artifact/status",
            headers=headers,
            content=payload,
        )

        res.raise_for_status()

        _, data = self.exc.get_payload(res.content)

        return ArtifactStatus(**json.loads(data))

    def wait(self, artifact: Artifact | ArtifactStatus, max_wait: int = 60, wait_time: int = 10) -> None:
        """Waiting loop for artifact status until completion or waiting time is
        reached.

        Args:
            artifact (Artifact | ArtifactStatus):
                Artifact or ArtifactStatus to wait for.
            max_wait (int, optional):
                Max waiting time in seconds.
                Defaults to 60.
            wait_time (int, optional):
                Wait interval in seconds.
                Defaults to 10.
        Raises:
            ValueError:
                When max waiting time is reached.
        """
        last_state = None

        start_time = time.time()
        while (status := self.status(artifact)).status != ArtifactJobStatus.COMPLETED:
            if status.status == last_state:
                LOGGER.info(".")
            else:
                last_state = status.status
                start_time = time.time()
                LOGGER.info(last_state)

            time.sleep(wait_time)

            if time.time() - start_time > max_wait:
                LOGGER.error("reached max wait time")
                raise ValueError("Reached max wait time")

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

        headers, payload = self.exc.create(wba.json())

        res = httpx.request(
            "GET",
            f"{self.server_url}/workbench/artifact",
            headers=headers,
            content=payload,
        )

        res.raise_for_status()

        _, data = self.exc.get_payload(res.content)

        return Artifact(**json.loads(data))

    def list_resources(self, artifact: Artifact) -> list[WorkbenchResource]:
        wba = WorkbenchArtifact(artifact_id=artifact.id)

        headers, payload = self.exc.create(wba.json())

        res = httpx.request(
            "GET",
            f"{self.server_url}/workbench/resource/list",
            headers=headers,
            content=payload,
        )

        res.raise_for_status()

        _, data = self.exc.get_payload(res.content)

        js = json.loads(data)

        return [WorkbenchResource(**d) for d in js]

    def get_resource(self, resource: WorkbenchResource) -> Any:
        headers, payload = self.exc.create(resource.json())

        with httpx.stream(
            "GET",
            f"{self.server_url}/workbench/resource",
            headers=headers,
            content=payload,
        ) as res:
            res.raise_for_status()

            data, _ = self.exc.stream_response(res.iter_bytes())

            obj = pickle.loads(data)

            return obj
