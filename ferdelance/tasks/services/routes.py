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
    """This router has direct access to local file system."""

    def __init__(
        self,
        component_id: str,
        private_key: str,
        remote_url: str,
        remote_public_key: str,
        is_local: bool = False,
    ) -> None:
        self.component_id: str = component_id
        self.remote: str = remote_url
        self.is_local: bool = is_local

        self.exc: Exchange = Exchange()
        self.exc.set_private_key(private_key)
        self.exc.set_remote_key(remote_public_key)

        LOGGER.info(
            f"component={self.component_id}: RouteService initialized for remote={self.remote} is_local={self.is_local}"
        )

    def reroute(
        self,
        remote_url: str,
        remote_public_key: str,
        is_local: bool = False,
    ) -> None:
        LOGGER.info(f"component={self.component_id}: rerouting from {self.remote} to {remote_url}")

        self.remote = remote_url
        self.is_local = is_local
        self.exc.set_remote_key(remote_public_key)

    def get_task_data(self, artifact_id: str, job_id: str) -> Task:
        LOGGER.info(f"JOB job={job_id}: getting task data")

        req = TaskRequest(artifact_id=artifact_id, job_id=job_id)

        headers, payload = self.exc.create(
            self.component_id,
            req.json(),
        )

        res = requests.get(
            f"{self.remote}/task",
            headers=headers,
            data=payload,
        )

        res.raise_for_status()

        _, content = self.exc.get_payload(res.content)

        task = Task(**json.loads(content))

        LOGGER.info(
            f"JOB job={job_id}: got task with "
            f"produced resource={task.produced_resource_id} "
            f"from n_resources={len(task.required_resources)}"
            f"will be sent to n_nodes={len(task.next_nodes)}"
        )

        return task

    def get_resource(self, producer_id: str, artifact_id: str, job_id: str, resource_id: str, iteration: int) -> Path:
        LOGGER.info(f"JOB job={job_id}: requesting resource={resource_id}")

        req = ResourceIdentifier(
            producer_id=producer_id,
            artifact_id=artifact_id,
            resource_id=resource_id,
            iteration=iteration,
        )

        headers, payload = self.exc.create(
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

            self.exc.stream_response_to_file(res, path)

            return path

    def post_resource(
        self, artifact_id: str, job_id: str, resource_id: str, path_in: Path | None = None, content: Any = None
    ) -> ResourceIdentifier:
        LOGGER.info(f"JOB job={job_id}: posting resource for ")

        nr = NewResource(
            artifact_id=artifact_id,
            job_id=job_id,
            resource_id=resource_id,
            file="attached",
        )

        if path_in is not None:
            path_out = path_in.parent / f"{path_in.name}.enc"

            checksum = self.exc.encrypt_file_for_remote(path_in, path_out)  # TODO: this should be optional!
            headers = self.exc.create_signed_header(
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
            headers, payload = self.exc.create(
                self.component_id,
                content,
                extra_headers=nr.dict(),
            )

            _, data = self.exc.stream(payload)

            res = requests.post(
                f"{self.remote}/resource",
                headers=headers,
                data=data,
            )

            res.raise_for_status()

        else:
            nr.file = "local"

            headers, _ = self.exc.create(
                self.component_id,
                extra_headers=nr.dict(),
            )

            res = requests.post(
                f"{self.remote}/resource",
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

        headers, payload = self.exc.create(
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

        headers, payload = self.exc.create(
            self.component_id,
            done.json(),
        )

        res = requests.post(
            f"{self.remote}/task/done",
            headers=headers,
            data=payload,
        )

        res.raise_for_status()
