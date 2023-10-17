from typing import Any
from abc import ABC, abstractmethod

from ferdelance.logging import get_logger
from ferdelance.schemas.models.metrics import Metrics
from ferdelance.schemas.resources import ResourceRequest
from ferdelance.schemas.tasks import TaskError, TaskParameters, TaskParametersRequest
from ferdelance.shared.exchange import Exchange

import json
import os
import pickle
import requests

LOGGER = get_logger(__name__)


class RouteService(ABC):
    @abstractmethod
    def get_params(self, artifact_id: str, job_id: str) -> TaskParameters:
        raise NotImplementedError()

    @abstractmethod
    def get_resource(self, artifact_id: str, job_id: str, resource_id: str) -> Any:
        raise NotImplementedError()

    @abstractmethod
    def post_result(
        self, artifact_id: str, job_id: str, path_in: str | None = None, content: Any = None
    ) -> ResourceRequest:
        raise NotImplementedError()

    @abstractmethod
    def post_metrics(self, artifact_id: str, job_id: str, metrics: Metrics):
        raise NotImplementedError()

    @abstractmethod
    def post_error(self, artifact_id: str, job_id: str, error: TaskError) -> None:
        raise NotImplementedError()

    @abstractmethod
    def update_artifact_with_resource(self, artifact_id: str, job_id: str, content: bytes) -> None:
        raise NotImplementedError()


class EncryptRouteService(RouteService):
    """This router has direct access to local file system."""

    def __init__(
        self,
        component_id: str,
        server_url: str,
        private_key: str,
        server_public_key: str,
    ) -> None:
        self.server: str = server_url

        self.component_id: str = component_id
        self.exc: Exchange = Exchange()
        self.exc.set_private_key(private_key)
        self.exc.set_remote_key(server_public_key)

    def get_params(self, artifact_id: str, job_id: str) -> TaskParameters:
        LOGGER.info(f"artifact={artifact_id}: getting context for job={job_id}")

        req = TaskParametersRequest(artifact_id=artifact_id, job_id=job_id)

        headers, payload = self.exc.create(
            self.component_id,
            req.json(),
        )

        res = requests.post(
            f"{self.server}/task/context",
            headers=headers,
            data=payload,
        )

        res.raise_for_status()

        _, content = self.exc.get_payload(res.content)

        context = TaskParameters(**json.loads(content))

        LOGGER.info(f"artifact={artifact_id}: context for jon={job_id} upload successfull")

        return context

    def get_resource(self, artifact_id: str, job_id: str, resource_id: str) -> Any:
        LOGGER.info(f"artifact={artifact_id}: requesting partial resource={resource_id} for job={job_id}")

        req = ResourceRequest(artifact_id=artifact_id, resource_id=resource_id)

        headers, payload = self.exc.create(
            self.component_id,
            req.json(),
        )

        with requests.get(
            f"{self.server}/task/resource/{resource_id}",
            headers=headers,
            data=payload,
            stream=True,
        ) as res:
            res.raise_for_status()

            content, _ = self.exc.stream_response(res.iter_content())

        return pickle.loads(content)

    def post_result(
        self, artifact_id: str, job_id: str, path_in: str | None = None, content: Any = None
    ) -> ResourceRequest:
        LOGGER.info(f"artifact={artifact_id}: posting resource for job={job_id}")

        if path_in is not None:
            path_out = f"{path_in}.enc"

            checksum = self.exc.encrypt_file_for_remote(path_in, path_out)
            headers = self.exc.create_signed_header(
                self.component_id,
                checksum,
                extra_headers={
                    "job_id": job_id,
                    "file": "attached",
                },
            )

            res = requests.post(
                f"{self.server}/task/resource",
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
                extra_headers={
                    "job_id": job_id,
                    "file": "attached",
                },
            )

            _, data = self.exc.stream(payload)

            res = requests.post(
                f"{self.server}/task/resource",
                headers=headers,
                data=data,
            )

            res.raise_for_status()

        else:
            headers, _ = self.exc.create(
                self.component_id,
                extra_headers={
                    "job_id": job_id,
                    "file": "local",
                },
            )

            res = requests.post(
                f"{self.server}/task/resource",
                headers=headers,
            )

            res.raise_for_status()

        _, payload = self.exc.get_payload(res.content)

        req = ResourceRequest(**json.loads(payload))

        LOGGER.info(f"artifact={artifact_id}: resource for job={job_id} upload successful")

        return req

    def post_metrics(self, artifact_id: str, job_id: str, metrics: Metrics):
        LOGGER.info(f"artifact={artifact_id}: posting metrics for job={job_id}")

        headers, payload = self.exc.create(self.component_id, metrics.json())

        res = requests.post(
            f"{self.server}/task/metrics",
            headers=headers,
            data=payload,
        )

        res.raise_for_status()

        LOGGER.info(
            f"artifact={metrics.artifact_id}: "
            f"metrics for job={metrics.job_id} from source={metrics.source} upload successful"
        )

    def post_error(self, artifact_id: str, job_id: str, error: TaskError) -> None:
        LOGGER.error(f"artifact={artifact_id} job={job_id}: error_message={error.message}")

        headers, payload = self.exc.create(self.component_id, error.json())

        res = requests.post(
            f"{self.server}/task/error",
            headers=headers,
            data=payload,
        )

        res.raise_for_status()

    def update_artifact_with_resource(self, artifact_id: str, job_id: str, content: bytes) -> None:
        LOGGER.info(f"artifact={artifact_id}: updating resource for job={job_id}")

        if content is not None:
            headers, payload = self.exc.create(
                self.component_id,
                content,
                extra_headers={
                    "job_id": job_id,
                    "file": "attached",
                },
            )

            _, data = self.exc.stream(payload)

            res = requests.post(
                f"{self.server}/task/update",
                headers=headers,
                data=data,
            )

            res.raise_for_status()

        LOGGER.info(f"artifact={artifact_id}: resource from job={job_id} update successful")
