from typing import Any

from ferdelance.config import config_manager
from ferdelance.core.metrics import Metrics
from ferdelance.logging import get_logger
from ferdelance.schemas.resources import NewResource, ResourceIdentifier
from ferdelance.shared.exchange import Exchange
from ferdelance.tasks.tasks import Task, TaskDone, TaskError, TaskRequest

from pathlib import Path

import json
import os
import requests

LOGGER = get_logger(__name__)


class RouteService:
    """A service to manage the transfer of payloads between two nodes."""

    def __init__(
        self,
        component_id: str,
        private_key: str,
        remote_url: str,
        remote_public_key: str,
        target_component_id: str,
        target_public_key: str,
        encrypt_payload: bool = True,
        is_local: bool = False,
    ) -> None:
        """
        Args:
            component_id (str):
                Identifier string of this component running the task.
            private_key (str):
                Private key in string format for this component running the task.
            remote_url (str):
                Base url of the remote node receiving the communication and data.
            remote_public_key (str):
                Public key in string format for the node receiving the communication data.
            target_component_id (str):
                Component id of the final receiver node of the encrypted data.
            target_public_key (str):
                Public key in string format for the node that is the final receiver of the
                encrypted data. This node can be the same value of `remote_public_key` in
                the case the remote node is also the target node.
            encrypt_payload (bool, optional):
                If True, the produced resources will be transferred to the remote node
                using the target_public_key, otherwise it will be transferred unencrypted.
                Defaults to True.
            is_local (bool, optional):
                Set to True when the remote node is the same node that is running the task.
                Defaults to False.
        """

        self.component_id: str = component_id
        self.target_component_id: str = target_component_id
        self.remote: str = remote_url

        self.encrypt_payload: bool = encrypt_payload
        self.is_local: bool = is_local

        # used for communication and header signing
        self.exc_comm: Exchange = Exchange()
        self.exc_comm.set_private_key(private_key)
        self.exc_comm.set_remote_key(remote_public_key)

        # used for payload encryption (when required)
        self.exc_target: Exchange = Exchange()
        self.exc_target.set_private_key(private_key)
        self.exc_target.set_remote_key(target_public_key)

        LOGGER.info(
            f"component={self.component_id}: RouteService initialized for "
            f"target={self.target_component_id} through remote={self.remote} "
            f"is_local={self.is_local}"
        )

    def reroute(
        self,
        remote_url: str,
        remote_public_key: str,
        is_local: bool = False,
    ) -> None:
        """Changes the destination of this route to another node.

        Args:
            remote_url (str):
                Url of the new route node.
            remote_public_key (str):
                Public key of the new remote node.
            is_local (bool, optional):
                Set to True if the remote node is located in the same machine
                of this worker task.
                Defaults to False.
        """
        LOGGER.info(f"component={self.component_id}: rerouting from {self.remote} to {remote_url}")

        self.remote = remote_url
        self.is_local = is_local
        self.exc_comm.set_remote_key(remote_public_key)

    def get_task_data(self, artifact_id: str, job_id: str) -> Task:
        """Contact the remote node to get the data for the given task.

        A task is identified by an artifact_id and a job_id.

        Args:
            artifact_id (str):
                Id of th artifact for the task to fetch.
            job_id (str):
                Id of the job for the task to fetch.

        Returns:
            Task:
                The descriptor for the task to execute.
        """
        LOGGER.info(f"JOB job={job_id}: getting task data")

        req = TaskRequest(artifact_id=artifact_id, job_id=job_id)

        headers, payload = self.exc_comm.create(
            self.component_id,
            req.json(),
        )

        res = requests.get(
            f"{self.remote}/task",
            headers=headers,
            data=payload,
        )

        res.raise_for_status()

        _, content = self.exc_comm.get_payload(res.content)

        task = Task(**json.loads(content))

        LOGGER.info(
            f"JOB job={job_id}: got task with "
            f"produced resource={task.produced_resource_id} "
            f"from n_resources={len(task.required_resources)} "
            f"will be sent to n_nodes={len(task.next_nodes)}"
        )

        return task

    def get_resource(
        self,
        producer_id: str,
        artifact_id: str,
        job_id: str,
        resource_id: str,
        iteration: int,
        encrypted: bool = True,
        CHUNK_SIZE: int = 4096,
    ) -> Path:
        """Contact the remote node to get a specific resource.

        The required parameters are specified by the content that can be fetch
        using the get_task_data method.

        Args:
            producer_id (str):
                Id of the component that produced the resource.
            artifact_id (str):
                Artifact id of the resource.
            job_id (str):
                Job id that produced the resource.
            resource_id (str):
                Id assigned to the requested resource.
            iteration (int):
                Iteration in which the resource have been produced.
            encrypted:
                If True, it is expected that te resource is encrypted and need
                to be decrypted before it can be used, otherwise no decryption
                will happen.
                Defaults to True.

        Returns:
            Path:
                Local path where the resource has been stored.
        """
        LOGGER.info(f"JOB job={job_id}: requesting resource={resource_id}")

        req = ResourceIdentifier(
            producer_id=producer_id,
            artifact_id=artifact_id,
            resource_id=resource_id,
            iteration=iteration,
        )

        headers, payload = self.exc_comm.create(
            self.component_id,
            req.json(),
        )

        with requests.get(
            f"{self.remote}/resource",
            headers=headers,
            data=payload,
            stream=True,
        ) as res:
            res.raise_for_status()

            path = config_manager.get().storage_job(artifact_id, job_id, iteration) / f"{resource_id}.pkl"

            # TODO: encryption can be retrieved through headers?

            if encrypted:
                self.exc_comm.stream_response_to_file(res, path)
            else:
                with open(path, "wb") as f:
                    for chunk in res.iter_content(chunk_size=CHUNK_SIZE):
                        f.write(chunk)

            return path

    def post_resource(
        self,
        artifact_id: str,
        job_id: str,
        resource_id: str,
        path_in: Path | None = None,
        content: Any = None,
        encryption: bool = True,
    ) -> ResourceIdentifier:
        LOGGER.info(f"JOB job={job_id}: posting resource to {self.remote}")

        nr = NewResource(
            target_id=self.target_component_id,
            artifact_id=artifact_id,
            job_id=job_id,
            resource_id=resource_id,
            file="attached",
        )

        if path_in is not None:
            path_out = path_in.parent / f"{path_in.name}.enc"

            checksum = self.exc_comm.encrypt_file_for_remote(path_in, path_out)  # TODO: this should be optional!
            headers = self.exc_comm.create_signed_header(
                self.component_id,
                checksum,
                extra_headers=nr.dict(),
            )

            res = requests.post(
                f"{self.remote}/resource",
                headers=headers,
                data=open(path_out, "rb"),
            )

            if os.path.exists(path_out):
                os.remove(path_out)

            res.raise_for_status()

        elif content is not None:
            headers, payload = self.exc_comm.create(
                self.component_id,
                content,
                extra_headers=nr.dict(),
            )

            _, data = self.exc_comm.stream(payload)

            res = requests.post(
                f"{self.remote}/resource",
                headers=headers,
                data=data,
            )

            res.raise_for_status()

        else:
            nr.file = "local"

            headers, _ = self.exc_comm.create(
                self.component_id,
                extra_headers=nr.dict(),
            )

            res = requests.post(
                f"{self.remote}/resource",
                headers=headers,
            )

            res.raise_for_status()

        _, payload = self.exc_comm.get_payload(res.content)

        req = ResourceIdentifier(**json.loads(payload))

        LOGGER.info(f"JOB job={job_id}: resource={resource_id} upload successful")

        return req

    def post_metrics(self, job_id: str, metrics: Metrics):
        LOGGER.info(f"JOB job={job_id}: posting metrics")

        headers, payload = self.exc_comm.create(
            self.component_id,
            metrics.json(),
        )

        res = requests.post(
            f"{self.remote}/task/metrics",
            headers=headers,
            data=payload,
        )

        res.raise_for_status()

        LOGGER.info(
            f"artifact={metrics.artifact_id}: "
            f"metrics for job={metrics.job_id} from source={metrics.source} upload successful"
        )

    def post_error(self, job_id: str, error: TaskError) -> None:
        LOGGER.error(f"JOB job={job_id}: error_message={error.message}")

        headers, payload = self.exc_comm.create(
            self.component_id,
            error.json(),
        )

        res = requests.post(
            f"{self.remote}/task/error",
            headers=headers,
            data=payload,
        )

        res.raise_for_status()

    def post_done(self, artifact_id: str, job_id: str) -> None:
        LOGGER.info(f"JOB job={job_id}: work done")

        done = TaskDone(
            artifact_id=artifact_id,
            job_id=job_id,
        )

        headers, payload = self.exc_comm.create(
            self.component_id,
            done.json(),
        )

        res = requests.post(
            f"{self.remote}/task/done",
            headers=headers,
            data=payload,
        )

        res.raise_for_status()
