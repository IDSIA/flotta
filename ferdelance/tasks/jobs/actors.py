from typing import Any

from abc import ABC, abstractmethod

from ferdelance.config import DataSourceConfiguration, get_logger
from ferdelance.client.state import DataConfig
from ferdelance.schemas.errors import TaskError
from ferdelance.schemas.artifacts import Artifact
from ferdelance.schemas.models import GenericModel
from ferdelance.schemas.estimators import GenericEstimator
from ferdelance.schemas.tasks import ExecutionResult
from ferdelance.tasks.jobs.execution import run_estimate, run_training
from ferdelance.tasks.jobs.routes import EncryptRouteService, RouteService, TaskParameters

import ray

LOGGER = get_logger(__name__)


class GenericJob(ABC):
    def __init__(
        self,
        artifact_id: str,
        job_id: str,
        server_url: str,
        token: str,
        private_key: str,
        server_public_key: str,
    ) -> None:
        self.routes_service: RouteService = EncryptRouteService(server_url, token, private_key, server_public_key)
        self.artifact_id: str = artifact_id
        self.job_id: str = job_id

    def __repr__(self) -> str:
        return f"Job artifact_id={self.artifact_id} job_id={self.job_id}"

    @abstractmethod
    def run(self):
        raise NotImplementedError()


class LocalJob(GenericJob):
    def __init__(
        self,
        artifact_id: str,
        job_id: str,
        server_url: str,
        token: str,
        private_key: str,
        server_public_key: str,
        workdir: str,
        datasources: list[dict[str, Any]],
    ) -> None:
        super().__init__(artifact_id, job_id, server_url, token, private_key, server_public_key)

        self.datasources: list[DataSourceConfiguration] = [DataSourceConfiguration(**d) for d in datasources]

        self.data = DataConfig(workdir, self.datasources)


@ray.remote
class TrainingJob(LocalJob):
    def __init__(
        self,
        artifact_id: str,
        job_id: str,
        server_url: str,
        token: str,
        private_key: str,
        server_public_key: str,
        workdir: str,
        datasources: list[dict[str, Any]],
    ) -> None:
        super().__init__(artifact_id, job_id, server_url, token, private_key, server_public_key, workdir, datasources)

    def __repr__(self) -> str:
        return f"Training{super().__repr__()}"

    def run(self):
        task: TaskParameters = self.routes_service.get_task_params(artifact_id=self.artifact_id, job_id=self.job_id)

        try:
            if task.artifact.is_model():
                res = self.train(task)

                for m in res.metrics:
                    m.job_id = res.job_id
                    self.routes_service.post_metrics(self.artifact_id, self.job_id, m)

                self.routes_service.post_result(self.artifact_id, self.job_id, path_in=res.path)

            else:
                raise ValueError("Artifact is not a model!")

        except Exception as e:
            LOGGER.error(e)
            LOGGER.exception(e)
            self.routes_service.post_error(
                task.job_id,
                task.artifact.id,
                TaskError(
                    job_id=task.job_id,
                    message=f"Malformed artifact: {e}",
                ),
            )

    def train(self, task: TaskParameters) -> ExecutionResult:
        res: ExecutionResult = run_training(self.data, task)
        return res


@ray.remote
class EstimationJob(LocalJob):
    def __init__(
        self,
        artifact_id: str,
        job_id: str,
        server_url: str,
        token: str,
        private_key: str,
        server_public_key: str,
        workdir: str,
        datasources: list[dict[str, Any]],
    ) -> None:
        super().__init__(artifact_id, job_id, server_url, token, private_key, server_public_key, workdir, datasources)

    def __repr__(self) -> str:
        return f"Estimation{super().__repr__()}"

    def run(self):
        task: TaskParameters = self.routes_service.get_task_params(self.artifact_id, self.job_id)

        if task.artifact.is_estimation():
            res = self.estimate(task)

            self.routes_service.post_result(self.artifact_id, self.job_id, path_in=res.path)

        else:
            self.routes_service.post_error(
                task.job_id,
                task.artifact.id,
                TaskError(
                    job_id=task.job_id,
                    message="Malformed artifact",
                ),
            )

    def estimate(self, task: TaskParameters) -> ExecutionResult:
        res: ExecutionResult = run_estimate(self.data, task)
        return res


@ray.remote
class AggregatingJob(GenericJob):
    def __init__(
        self,
        artifact_id: str,
        job_id: str,
        server_url: str,
        token: str,
        private_key: str,
        server_public_key: str,
    ) -> None:
        super().__init__(artifact_id, job_id, server_url, token, private_key, server_public_key)

    def __repr__(self) -> str:
        return f"Aggregating{super().__repr__()}"

    def run(self):
        task: TaskParameters = self.routes_service.get_task_params(self.artifact_id, self.job_id)

        content = self.aggregate(task)

        self.routes_service.post_result(self.artifact_id, self.job_id, content=content)

    def aggregate_estimator(self, artifact: Artifact, result_ids: list[str]) -> GenericEstimator:
        agg = artifact.get_estimator()

        base: Any = None

        for result_id in result_ids:
            partial: GenericEstimator = self.routes_service.get_result(artifact.id, self.job_id, result_id)

            if base is None:
                base = partial
            else:
                base = agg.aggregate(base, partial)

        LOGGER.info(f"artifact_id={artifact.id}: aggregated {len(result_ids)} estimator(s)")

        return base

    def aggregate_model(self, artifact: Artifact, result_ids: list[str]) -> GenericModel:
        agg = artifact.get_model()
        strategy = artifact.get_strategy()

        base: Any = None

        for result_id in result_ids:
            partial: GenericModel = self.routes_service.get_result(artifact.id, self.job_id, result_id)

            if base is None:
                base = partial
            else:
                base = agg.aggregate(strategy, base, partial)

        LOGGER.info(f"artifact_id={artifact.id}: aggregated {len(result_ids)} model(s)")

        return base

    def aggregate(self, task: TaskParameters) -> Any:
        artifact: Artifact = task.artifact
        result_ids: list[str] = task.content_ids

        if artifact.is_estimation():
            base = self.aggregate_estimator(artifact, result_ids)

        elif artifact.is_model():
            base = self.aggregate_model(artifact, result_ids)

        else:
            raise ValueError(f"Unsupported artifact_id={self.job_id}")

        return base
