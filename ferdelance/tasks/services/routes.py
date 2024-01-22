from typing import Any

from ferdelance.config import config_manager
from ferdelance.core.metrics import Metrics
from ferdelance.logging import get_logger
from ferdelance.schemas.resources import NewResource, ResourceIdentifier
from ferdelance.security.algorithms import Algorithm
from ferdelance.security.exchange import Exchange
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
        remote_id: str,
        remote_url: str,
        remote_public_key: str,
        is_local: bool = False,
    ) -> None:
        """
        Args:
            component_id (str):
                Identifier string of this component running the task.
            private_key (str):
                Private key in string format for this component running the task.
            remote_id (str):
                Component id of the remote node used for communication.
            remote_url (str):
                Base url of the remote node receiving the communication and data.
            remote_public_key (str):
                Public key in string format for the node receiving the communication data.
            is_local (bool, optional):
                Set to True when the remote node is the same node that is running the task.
                Defaults to False.
        """

        self.component_id: str = component_id

        self.remote_id: str = remote_id
        self.remote_url: str = remote_url
        self.remote_public_key: str = remote_public_key

        self.is_local: bool = is_local

        # used for communication and header signing
        self.exc: Exchange = Exchange()
        self.exc.set_private_key(private_key)
        self.exc.set_remote_key(remote_public_key)

        LOGGER.info(
            f"component={self.component_id}: RouteService initialized for "
            f"remote={self.remote_url} is_local={self.is_local}"
        )

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

        headers, payload = self.exc.create(
            self.component_id,
            self.remote_id,
            req.json(),
        )

        res = requests.get(
            f"{self.remote_url}/task",
            headers=headers,
            data=payload,
        )

        res.raise_for_status()

        _, content = self.exc.get_payload(res.content)

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

        headers, payload = self.exc.create(
            self.component_id,
            producer_id,  # TODO: check if we need the producer or the remote id
            req.json(),
        )

        with requests.get(
            f"{self.remote_url}/resource",
            headers=headers,
            data=payload,
            stream=True,
        ) as res:
            res.raise_for_status()

            headers = self.exc.get_headers(res.headers.get("Signature", ""))

            prev_algo = self.exc.algorithm
            self.exc.algorithm = Algorithm[headers.encryption]

            path = config_manager.get().storage_job(artifact_id, job_id, iteration) / f"{resource_id}.pkl"

            it = res.iter_content(chunk_size=CHUNK_SIZE)

            self.exc.stream_response_to_file(it, path)

            self.exc.algorithm = prev_algo

            return path

    def post_resource(
        self,
        target_id: str,
        artifact_id: str,
        job_id: str,
        resource_id: str,
        path_in: Path | None = None,
        content: Any = None,
    ) -> ResourceIdentifier:
        LOGGER.info(f"JOB job={job_id}: posting resource to {self.remote_url}")

        nr = NewResource(
            target_id=target_id,
            artifact_id=artifact_id,
            job_id=job_id,
            resource_id=resource_id,
            file="attached",
        )

        if path_in is not None:
            path_out = path_in.parent / f"{path_in.name}.enc"

            checksum = self.exc.encrypt_file_for_remote(path_in, path_out)  # TODO: this should be optional!
            headers = self.exc.create_signed_headers(
                self.component_id,
                checksum,
                target_id,
                extra_headers=nr.dict(),
            )

            res = requests.post(
                f"{self.remote_url}/resource",
                headers=headers,
                data=open(path_out, "rb"),
            )

            if os.path.exists(path_out):
                os.remove(path_out)

            res.raise_for_status()

        elif content is not None:
            headers, payload = self.exc.create(
                self.component_id,
                target_id,
                content,
                extra_headers=nr.dict(),
            )

            _, data = self.exc.encrypt_to_stream(payload)

            res = requests.post(
                f"{self.remote_url}/resource",
                headers=headers,
                data=data,
            )

            res.raise_for_status()

        else:
            nr.file = "local"

            headers, _ = self.exc.create(
                self.component_id,
                target_id,
                extra_headers=nr.dict(),
            )

            res = requests.post(
                f"{self.remote_url}/resource",
                headers=headers,
            )

            res.raise_for_status()

        _, payload = self.exc.get_payload(res.content)

        req = ResourceIdentifier(**json.loads(payload))

        LOGGER.info(f"JOB job={job_id}: resource={resource_id} upload successful")

        return req

    def post_metrics(self, job_id: str, metrics: Metrics):
        LOGGER.info(f"JOB job={job_id}: posting metrics")

        headers, payload = self.exc.create(
            self.component_id,
            self.remote_id,
            metrics.json(),
        )

        res = requests.post(
            f"{self.remote_url}/task/metrics",
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

        headers, payload = self.exc.create(
            self.component_id,
            self.remote_id,
            error.json(),
        )

        res = requests.post(
            f"{self.remote_url}/task/error",
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

        headers, payload = self.exc.create(
            self.component_id,
            self.remote_id,
            done.json(),
        )

        res = requests.post(
            f"{self.remote_url}/task/done",
            headers=headers,
            data=payload,
        )

        res.raise_for_status()
