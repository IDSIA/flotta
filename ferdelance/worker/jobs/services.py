from typing import Any

from abc import ABC, abstractclassmethod

from ferdelance.client.config import DataConfig
from ferdelance.schemas.client import DataSourceConfig
from ferdelance.schemas.errors import TaskError
from ferdelance.schemas.artifacts import Artifact
from ferdelance.schemas.models import GenericModel
from ferdelance.schemas.estimators import GenericEstimator
from ferdelance.schemas.worker import ExecutionResult
from ferdelance.worker.jobs.execution import run_estimate, run_training
from ferdelance.worker.jobs.routes import (
    RouteService,
    TaskArguments,
    TaskExecutionParameters,
    TaskAggregationParameters,
)


import logging

print("enter", __name__)

LOGGER = logging.getLogger(__name__)


class JobService(ABC):
    def __init__(self) -> None:
        self.routes_service: RouteService
        self.artifact_id: str
        self.job_id: str

    def setup(self, args: TaskArguments, routes_service: RouteService) -> None:
        self.routes_service: RouteService = routes_service
        self.artifact_id: str = args.artifact_id
        self.job_id: str = args.job_id

    @abstractclassmethod
    def run(self):
        raise NotImplementedError()


class LocalJobService(JobService):
    def __init__(self) -> None:
        super().__init__()

        self.data: DataConfig

    def setup(self, args: TaskArguments, routes_service: RouteService) -> None:
        super().setup(args, routes_service)

        datasources: list[DataSourceConfig] = [DataSourceConfig(**d) for d in args.datasources]

        self.data = DataConfig(args.workdir, datasources)


class TrainingJobService(LocalJobService):
    def run(self):
        task: TaskExecutionParameters = self.routes_service.get_task_execution_params(
            artifact_id=self.artifact_id, job_id=self.job_id
        )

        if task.artifact.is_model():
            res = self.train(task)

            for m in res.metrics:
                m.job_id = res.job_id
                self.routes_service.post_metrics(self.artifact_id, self.job_id, m)

            self.routes_service.post_result(res.job_id, res.path)

        else:
            self.routes_service.post_error(
                task.job_id,
                task.artifact.id,
                TaskError(
                    job_id=task.job_id,
                    message="Malformed artifact",
                ),
            )

    def train(self, task: TaskExecutionParameters) -> ExecutionResult:
        res: ExecutionResult = run_training(self.data, task)
        return res


class EstimationJobService(LocalJobService):
    def run(self):
        task: TaskExecutionParameters = self.routes_service.get_task_execution_params(self.artifact_id, self.job_id)

        if task.artifact.is_estimation():
            res = self.estimate(task)

            self.routes_service.post_result(res.job_id, res.path)

        else:
            self.routes_service.post_error(
                task.job_id,
                task.artifact.id,
                TaskError(
                    job_id=task.job_id,
                    message="Malformed artifact",
                ),
            )

    def estimate(self, task: TaskExecutionParameters) -> ExecutionResult:
        res: ExecutionResult = run_estimate(self.data, task)
        return res


class AggregatingJobService(JobService):
    def run(self):
        task: TaskAggregationParameters = self.routes_service.get_task_aggregation_params(self.artifact_id, self.job_id)

        content = self.aggregate(task)

        self.routes_service.post_result(self.artifact_id, self.job_id, content)

    def aggregate_estimator(self, artifact: Artifact, result_ids: list[str]) -> GenericEstimator:
        agg = artifact.get_estimator()

        base: Any = None

        for result_id in result_ids:
            partial: GenericEstimator = self.routes_service.get_result(artifact.id, self.job_id, result_id)

            if base is None:
                base = partial
            else:
                base = agg.aggregate(base, partial)

        logging.info(f"artifact_id={artifact.id}: aggregated {len(result_ids)} estimator(s)")

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

        logging.info(f"artifact_id={artifact.id}: aggregated {len(result_ids)} model(s)")

        return base

    def aggregate(self, task) -> Any:
        artifact: Artifact = task.artifact
        result_ids: list[str] = task.result_ids

        if artifact.is_estimation():
            base = self.aggregate_estimator(artifact, result_ids)

        elif artifact.is_model():
            base = self.aggregate_model(artifact, result_ids)

        else:
            raise ValueError(f"Unsupported artifact_id={self.job_id}")

        return base
