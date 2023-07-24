from typing import Any
from abc import ABC, abstractclassmethod

from ferdelance.schemas.errors import TaskError
from ferdelance.schemas.models.metrics import Metrics
from ferdelance.schemas.updates import UpdateExecute
from ferdelance.schemas.worker import TaskArguments, TaskExecutionParameters, TaskAggregationParameters
from ferdelance.shared.exchange import Exchange

import os
import json
import logging
import pickle
import requests

LOGGER = logging.getLogger(__name__)


class RouteService(ABC):
    @abstractclassmethod
    def get_task_execution_params(self, artifact_id: str, job_id: str) -> TaskExecutionParameters:
        raise NotImplementedError()

    @abstractclassmethod
    def get_task_aggregation_params(self, artifact_id: str, job_id: str) -> TaskAggregationParameters:
        raise NotImplementedError()

    @abstractclassmethod
    def get_result(self, artifact_id: str, job_id: str, result_id: str) -> Any:
        raise NotImplementedError()

    @abstractclassmethod
    def post_result(self, artifact_id: str, job_id: str, path_in: str | None = None, content: Any = None):
        raise NotImplementedError()

    @abstractclassmethod
    def post_metrics(self, artifact_id: str, job_id: str, metrics: Metrics):
        raise NotImplementedError()

    @abstractclassmethod
    def post_error(self, artifact_id: str, job_id: str, error: TaskError) -> None:
        raise NotImplementedError()


class LocalRouteService(RouteService):
    """This router has direct access to local file system."""

    def __init__(self, args: TaskArguments) -> None:
        self.exc: Exchange = Exchange()
        self.exc.load_key(args.private_key_location)
        self.exc.set_remote_key(args.server_public_key)
        self.exc.set_token(args.token)

        self.server: str = args.server_url

    def get_task_execution_params(self, artifact_id: str, job_id: str) -> TaskExecutionParameters:
        LOGGER.info(f"artifact_id={artifact_id} job_id={job_id}: requesting task execution parameters")

        task = UpdateExecute(
            action="",
            artifact_id=artifact_id,
            job_id=job_id,
        )

        res = requests.get(
            f"{self.server}/client/task",
            headers=self.exc.headers(),
            data=self.exc.create_payload(task.dict()),
        )

        res.raise_for_status()

        return TaskExecutionParameters(**self.exc.get_payload(res.content))

    def post_result(self, artifact_id: str, job_id: str, path_in: str | None = None, content: Any = None):
        LOGGER.info(f"artifact_id={artifact_id} job_id={job_id}: posting result")

        if path_in is not None:
            path_out = f"{path_in}.enc"

            self.exc.encrypt_file_for_remote(path_in, path_out)

            res = requests.post(
                f"{self.server}/client/result/{job_id}",
                headers=self.exc.headers(),
                data=open(path_out, "rb"),
            )

            if os.path.exists(path_out):
                os.remove(path_out)

            res.raise_for_status()

        elif content is not None:
            res = requests.post(
                f"{self.server}/worker/result/{job_id}",
                headers=self.exc.headers(),
                files={
                    "file": self.exc.create_payload(
                        {
                            "data": pickle.dumps(content),
                        }
                    ),
                },
            )

            res.raise_for_status()

        else:
            raise ValueError("No data to send!")

        LOGGER.info(f"artifact_id={artifact_id}: result from source={path_in} for job_id={job_id} upload successful")

    def post_metrics(self, artifact_id: str, job_id: str, metrics: Metrics):
        LOGGER.info(f"artifact_id={artifact_id} job_id={job_id}: posting metrics")
        res = requests.post(
            f"{self.server}/client/metrics",
            headers=self.exc.headers(),
            data=self.exc.create_payload(metrics.dict()),
        )

        res.raise_for_status()

        LOGGER.info(
            f"job_id={metrics.job_id}: "
            f"metrics for artifact_id={metrics.artifact_id} from source={metrics.source} upload successful"
        )

    def post_error(self, artifact_id: str, job_id: str, error: TaskError) -> None:
        LOGGER.error(f"artifact_id={artifact_id} job_id={job_id}: error_message={error.message}")
        res = requests.post(
            f"{self.server}/client/error/{job_id}",
            headers=self.exc.headers(),
            data=self.exc.create_payload(error.dict()),
        )

        res.raise_for_status()


class MemoryRouteService(RouteService):
    def __init__(self, args: TaskArguments) -> None:
        self.server = args.server_url
        self.token = args.token

    def headers(self):
        return {"Authorization": f"Bearer {self.token}"}

    def get_task_aggregation_params(self, artifact_id: str, job_id: str) -> TaskAggregationParameters:
        logging.info(f"artifact_id={artifact_id} job_id={job_id}: fetching task data")

        res = requests.get(
            f"{self.server}/worker/task/{job_id}",
            headers=self.headers(),
        )

        res.raise_for_status()

        return TaskAggregationParameters(**res.json())

    def get_result(self, artifact_id: str, job_id: str, result_id: str) -> Any:
        logging.info(f"artifact_id={artifact_id} job_id={job_id}: : requesting partial result_id={result_id}")

        res = requests.get(
            f"{self.server}/worker/result/{result_id}",
            headers=self.headers(),
        )

        res.raise_for_status()

        return pickle.loads(res.content)

    def post_result(self, artifact_id: str, job_id: str, content: Any) -> None:
        logging.info(f"artifact_id={artifact_id} job_id={job_id}: : posting aggregated result for job_id={job_id}")

        res = requests.post(
            f"{self.server}/worker/result/{job_id}",
            headers=self.headers(),
            files={
                "file": pickle.dumps(content),
            },
        )

        res.raise_for_status()

    def post_error(self, artifact_id: str, job_id: str, error: TaskError) -> None:
        logging.info(f"artifact_id={artifact_id} job_id={job_id}: : posting error")

        res = requests.post(
            f"{self.server}/worker/error/",
            headers=self.headers(),
            data=json.dumps(error.dict()),
        )

        res.raise_for_status()
