from typing import Any
from abc import ABC, abstractmethod

from ferdelance.logging import get_logger
from ferdelance.schemas.errors import TaskError
from ferdelance.schemas.models.metrics import Metrics
from ferdelance.schemas.tasks import TaskParameters, TaskParametersRequest
from ferdelance.shared.exchange import Exchange

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
        server_url: str,
        private_key: str,
        server_public_key: str,
    ) -> None:
        self.server: str = server_url

        self.exc: Exchange = Exchange()
        self.exc.set_private_key(private_key)
        self.exc.set_remote_key(server_public_key)

    def get_task_params(self, artifact_id: str, job_id: str) -> TaskParameters:
        LOGGER.info(f"artifact_id={artifact_id}: requesting task execution parameters for job_id={job_id}")

        task = TaskParametersRequest(
            artifact_id=artifact_id,
            job_id=job_id,
        )

        res = requests.get(
            f"{self.server}/task/params",
            headers=self.exc.headers(),
            data=self.exc.create_payload(task.dict()),
        )

        res.raise_for_status()

        return TaskParameters(**self.exc.get_payload(res.content))

    def get_result(self, artifact_id: str, job_id: str, result_id: str) -> Any:
        LOGGER.info(f"artifact_id={artifact_id}: requesting partial result_id={result_id} for job_id={job_id}")

        with requests.get(
            f"{self.server}/task/result/{result_id}",
            headers=self.exc.headers(),
            stream=True,
        ) as res:
            res.raise_for_status()

            content, _ = self.exc.stream_response(res.iter_content())

        return pickle.loads(content)

    def post_result(self, artifact_id: str, job_id: str, path_in: str | None = None, content: Any = None):
        LOGGER.info(f"artifact_id={artifact_id}: posting result for job_id={job_id}")

        if path_in is not None:
            path_out = f"{path_in}.enc"

            self.exc.encrypt_file_for_remote(path_in, path_out)

            res = requests.post(
                f"{self.server}/task/result/{job_id}",
                headers=self.exc.headers(),
                data=open(path_out, "rb"),
            )

            if os.path.exists(path_out):
                os.remove(path_out)

            res.raise_for_status()

        elif content is not None:
            data = pickle.dumps(content)

            res = requests.post(
                f"{self.server}/task/result/{job_id}",
                headers=self.exc.headers(),
                data=self.exc.stream(data),
            )

            res.raise_for_status()

        else:
            raise ValueError("No data to send!")

        LOGGER.info(f"artifact_id={artifact_id}: result from source={path_in} for job_id={job_id} upload successful")

    def post_metrics(self, artifact_id: str, job_id: str, metrics: Metrics):
        LOGGER.info(f"artifact_id={artifact_id}: posting metrics for job_id={job_id}")
        res = requests.post(
            f"{self.server}/task/metrics",
            headers=self.exc.headers(),
            data=self.exc.create_payload(metrics.dict()),
        )

        res.raise_for_status()

        LOGGER.info(
            f"artifact_id={metrics.artifact_id}: "
            f"metrics for job_id={metrics.job_id} from source={metrics.source} upload successful"
        )

    def post_error(self, artifact_id: str, job_id: str, error: TaskError) -> None:
        LOGGER.error(f"artifact_id={artifact_id} job_id={job_id}: error_message={error.message}")
        res = requests.post(
            f"{self.server}/task/error",
            headers=self.exc.headers(),
            data=self.exc.create_payload(error.dict()),
        )

        res.raise_for_status()
