from typing import Any

from ferdelance.core.metrics import Metrics
from ferdelance.logging import get_logger
from ferdelance.schemas.resources import NewResource, ResourceIdentifier
from ferdelance.security.algorithms import Algorithm
from ferdelance.security.exchange import Exchange
from ferdelance.tasks.tasks import Task, TaskDone, TaskError, TaskRequest

from pathlib import Path

import httpx
import json
import os

LOGGER = get_logger(__name__)


class RouteService:
    """A service to manage the transfer of payloads between two nodes."""

    def __init__(
        self,
        component_id: str,
        private_key: str,
    ) -> None:
        """
        Args:
            component_id (str):
                Identifier of the component running the task.
            private_key (str):
                Private key in string format for the component running the task.
        """

        self.component_id: str = component_id
        self.remote_url: str = ""

        # used for communication and header signing
        self.exc: Exchange = Exchange(component_id, private_key=private_key)

    def change_route(
        self,
        target_id: str,
        target_public_key: str,
        remote_url: str,
        remote_public_key: str | None = None,
    ) -> None:
        """Change the parameters used for connecting to the current route.

        Args:
            target_id (str):
                Component id of the remote that will receive the payload.
            target_public_key (str):
                Public key in string format for the node receiving the payload.
            remote_url (str):
                Base url of the remote node receiving the payload.
            remote_public_key (str):
                Public key in string format for the node receiving the payload.
                Defaults to None.
        """
        self.remote_url = remote_url.rstrip("/")

        self.exc.set_remote_key(target_id, target_public_key)

        LOGGER.info(f"component={self.component_id}: changed route to remote={self.remote_url}")

        if remote_public_key is not None and target_public_key != remote_public_key:
            self.exc.set_proxy_key(remote_public_key)
            LOGGER.info(f"component={self.component_id}: using proxy")

    def _stream_get(self, url: str, headers: dict[str, str], data: Any = None):
        return httpx.stream(
            "GET",
            f"{self.remote_url}{url}",
            headers=headers,
            content=data,
        )

    def _get(self, url: str, headers: dict[str, str], data: Any = None) -> httpx.Response:
        return httpx.request(
            "GET",
            f"{self.remote_url}{url}",
            headers=headers,
            content=data,
        )

    def _post(self, url: str, headers: dict[str, str], data: Any = None) -> httpx.Response:
        return httpx.post(
            f"{self.remote_url}{url}",
            headers=headers,
            content=data,
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

        headers, payload = self.exc.create(req.model_dump_json())

        res = self._get(
            "/task/",
            headers=headers,
            data=payload,
        )

        res.raise_for_status()

        _, content = self.exc.get_payload(res.content)

        task = Task(**json.loads(content))

        LOGGER.info(
            f"JOB job={job_id}: got task with "
            f"produced resource={task.produced_resource_id} "
            f"and n_resources={len(task.required_resources)} "
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
        path_out: Path,
        CHUNK_SIZE: int = 4096,
    ) -> None:
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

        headers, payload = self.exc.create(req.model_dump_json())

        with self._stream_get(
            "/resource/",
            headers=headers,
            data=payload,
        ) as res:
            res.raise_for_status()

            headers = self.exc.get_headers(res.headers.get("Signature", ""))

            prev_algo = self.exc.algorithm
            self.exc.algorithm = Algorithm[headers.encryption]

            it = res.iter_bytes(chunk_size=CHUNK_SIZE)

            self.exc.stream_response_to_file(it, path_out)

            self.exc.algorithm = prev_algo

    def post_resource(
        self,
        artifact_id: str,
        job_id: str,
        resource_id: str,
        path_in: Path | None = None,
        content: Any = None,
    ) -> ResourceIdentifier:
        """Send a produced resource to a node.

        The required parameters are specified by the content that can be fetch
        using the get_task_data method.

        Args:
            artifact_id (str):
                Artifact id of the resource.
            job_id (str):
                Job id that produced the resource.
            resource_id (str):
                Id assigned to the requested resource.
            path_in (Path | None, optional):
                Path to the resource saved locally. If set, the content will be
                read from this path.
                Defaults to None.
            content (Any, optional):
                Content to be send. If set, this parameter will be converted to
                string and sent to the remote node.
                Defaults to None.

        Returns:
            ResourceIdentifier:
                The response from the remote node.
        """
        LOGGER.info(f"JOB job={job_id}: posting resource to {self.remote_url}")

        nr = NewResource(
            artifact_id=artifact_id,
            job_id=job_id,
            resource_id=resource_id,
            file="attached",
        )

        if path_in is not None:
            path_out = path_in.parent / f"{path_in.name}.enc"

            checksum = self.exc.encrypt_file_for_remote(path_in, path_out)
            headers = self.exc.create_signed_headers(
                checksum,
                extra_headers=nr.model_dump(),
            )

            res = self._post(
                "/resource/",
                headers=headers,
                data=open(path_out, "rb"),
            )

            if os.path.exists(path_out):
                os.remove(path_out)

        elif content is not None:
            headers, payload = self.exc.create(extra_headers=nr.model_dump())

            _, data = self.exc.encrypt_to_stream(payload)

            res = self._post(
                "/resource/",
                headers=headers,
                data=data,
            )

        else:
            nr.file = "local"

            headers, _ = self.exc.create(extra_headers=nr.model_dump())

            res = self._post(
                "/resource/",
                headers=headers,
            )

        res.raise_for_status()

        _, payload = self.exc.get_payload(res.content)

        req = ResourceIdentifier(**json.loads(payload))

        LOGGER.info(f"JOB job={job_id}: resource={resource_id} upload successful")

        return req

    def post_metrics(self, job_id: str, metrics: Metrics) -> None:
        """Send metrics and evaluation results to the remote node.

        Args:
            job_id (str):
                Job id that produced the metrics.
            metrics (Metrics):
                The evaluation metrics to send to the remote node.
        """
        LOGGER.info(f"JOB job={job_id}: posting metrics")

        headers, payload = self.exc.create(metrics.model_dump_json())

        res = self._post(
            "/task/metrics",
            headers=headers,
            data=payload,
        )

        res.raise_for_status()

        LOGGER.info(
            f"artifact={metrics.artifact_id}: "
            f"metrics for job={metrics.job_id} from source={metrics.source} upload successful"
        )

    def post_error(self, job_id: str, error: TaskError) -> None:
        """Send an error message to the remote node

        Args:
            job_id (str):
                Job id that produced the metrics.
            error (TaskError):
                Error message to send to the remote node.
        """
        LOGGER.error(f"JOB job={job_id}: error_message={error.message}")

        headers, payload = self.exc.create(error.model_dump_json())

        res = self._post(
            "/task/error",
            headers=headers,
            data=payload,
        )

        res.raise_for_status()

    def post_done(self, artifact_id: str, job_id: str) -> None:
        """Notifies the successful completion of the assigned job.

        Args:
            artifact_id (str):
                Id of th artifact for the task to fetch.
            job_id (str):
                Job id that produced the metrics.
        """
        LOGGER.info(f"JOB job={job_id}: work done")

        done = TaskDone(
            artifact_id=artifact_id,
            job_id=job_id,
        )

        headers, payload = self.exc.create(done.model_dump_json())

        res = self._post(
            "/task/done",
            headers=headers,
            data=payload,
        )

        res.raise_for_status()

        _, payload = self.exc.get_payload(res.content)
        content = json.loads(payload)

        print(content)
