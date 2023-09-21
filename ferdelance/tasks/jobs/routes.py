from typing import Any
from abc import ABC, abstractmethod

from ferdelance.logging import get_logger
from ferdelance.schemas.models.metrics import Metrics
from ferdelance.schemas.tasks import TaskParameters, TaskParametersRequest, TaskError
from ferdelance.shared.exchange import Exchange

import json
import os
import pickle
import requests

LOGGER = get_logger(__name__)


class RouteService(ABC):
    @abstractmethod
    def get_task_params(self, artifact_id: str, job_id: str) -> TaskParameters:
        raise NotImplementedError()

    @abstractmethod
    def get_result(self, artifact_id: str, job_id: str, result_id: str) -> Any:
        raise NotImplementedError()

    @abstractmethod
    def post_result(self, artifact_id: str, job_id: str, path_in: str | None = None, content: Any = None):
        raise NotImplementedError()

    @abstractmethod
    def post_metrics(self, artifact_id: str, job_id: str, metrics: Metrics):
        raise NotImplementedError()

    @abstractmethod
    def post_error(self, artifact_id: str, job_id: str, error: TaskError) -> None:
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

    def get_task_params(self, artifact_id: str, job_id: str) -> TaskParameters:
        LOGGER.info(f"artifact={artifact_id}: requesting task execution parameters for job={job_id}")

        task = TaskParametersRequest(
            artifact_id=artifact_id,
            job_id=job_id,
        )

        headers, payload = self.exc.create(self.component_id, task.json())

        res = requests.get(
            f"{self.server}/task/params",
            headers=headers,
            data=payload,
        )

        res.raise_for_status()

        _, data = self.exc.get_payload(res.content)

        return TaskParameters(**json.loads(data))

    def get_result(self, artifact_id: str, job_id: str, result_id: str) -> Any:
        LOGGER.info(f"artifact={artifact_id}: requesting partial result={result_id} for job={job_id}")

        headers, _ = self.exc.create(self.component_id)

        with requests.get(
            f"{self.server}/task/result/{result_id}",
            headers=headers,
            stream=True,
        ) as res:
            res.raise_for_status()

            content, _ = self.exc.stream_response(res.iter_content())

        return pickle.loads(content)

    def post_result(self, artifact_id: str, job_id: str, path_in: str | None = None, content: Any = None):
        LOGGER.info(f"artifact={artifact_id}: posting result for job={job_id}")

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
                f"{self.server}/task/result/{job_id}",
                headers=headers,
                data=open(path_out, "rb"),
            )

            if os.path.exists(path_out):
                os.remove(path_out)

            res.raise_for_status()

            LOGGER.info(f"artifact={artifact_id}: result for job={job_id} from source={path_in} upload successful")

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
                f"{self.server}/task/result/{job_id}",
                headers=headers,
                data=data,
            )

            res.raise_for_status()

            LOGGER.info(f"artifact={artifact_id}: result for job={job_id} upload successful")

        else:
            headers, payload = self.exc.create(
                self.component_id,
                content,
                extra_headers={
                    "job_id": job_id,
                    "file": "local",
                },
            )

            _, data = self.exc.stream(payload)

            res = requests.post(
                f"{self.server}/task/result/{job_id}",
                headers=headers,
                data=data,
            )

            res.raise_for_status()

            LOGGER.info(f"artifact={artifact_id}: result for job={job_id} upload successful")

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
