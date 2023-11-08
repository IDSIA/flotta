from typing import Any

from ferdelance.config import config_manager
from ferdelance.core.artifacts import Artifact
from ferdelance.core.model import Model
from ferdelance.logging import get_logger
from ferdelance.core.estimators import Estimator
from ferdelance.schemas.tasks import TaskDone, TaskParameters
from ferdelance.tasks.jobs.generic import GenericJob

import ray


LOGGER = get_logger(__name__)


@ray.remote
class AggregatingJob(GenericJob):
    def __init__(
        self,
        component_id: str,
        artifact_id: str,
        job_id: str,
        server_url: str,
        private_key: str,
        server_public_key: str,
    ) -> None:
        super().__init__(component_id, artifact_id, job_id, server_url, private_key, server_public_key)

    def __repr__(self) -> str:
        return f"Aggregating{super().__repr__()}"

    def run(self):
        context: TaskParameters = self.routes_service.get_params(self.artifact_id, self.job_id)

        res = self.aggregate(context)

        self.routes_service.post_result(self.artifact_id, self.job_id, path_in=res.resource_path)

    def aggregate_estimator(self, artifact: Artifact, resource_ids: list[str]) -> Estimator:
        agg = artifact.get_estimator()

        base: Any = None

        for resource_id in resource_ids:
            partial: Estimator = self.routes_service.get_resource(artifact.id, self.job_id, resource_id)

            if base is None:
                base = partial
            else:
                base = agg.aggregate(base, partial)

        LOGGER.info(f"artifact={artifact.id}: aggregated {len(resource_ids)} estimator(s)")

        return base

    def aggregate_model(self, artifact: Artifact, resource_ids: list[str]) -> Model:
        agg = artifact.get_model()
        strategy = artifact.get_strategy()

        base: Any = None

        for resource_id in resource_ids:
            partial: Model = self.routes_service.get_resource(artifact.id, self.job_id, resource_id)

            if base is None:
                base = partial
            else:
                base = agg.aggregate(strategy, base, partial)

        LOGGER.info(f"artifact={artifact.id}: aggregated {len(resource_ids)} model(s)")

        return base

    def aggregate(self, context: TaskParameters) -> TaskDone:
        artifact: Artifact = context.artifact
        resource_ids: list[str] = context.data["content_ids"]

        is_estimation = artifact.is_estimation()
        is_model = artifact.is_model()

        path: str = config_manager.get().store_resource(
            artifact.id,
            self.job_id,
            context.iteration,
            False,
            True,
            is_model,
            is_estimation,
        )

        base: Estimator | Model

        if is_estimation:
            base = self.aggregate_estimator(artifact, resource_ids)

            save_estimator(base, path)

            return TaskDone(
                job_id=context.job_id,
                resource_path=None,
                is_estimate=True,
                is_aggregation=True,
            )

        if is_model:
            base = self.aggregate_model(artifact, resource_ids)

            base.save(path)

            return TaskDone(
                job_id=context.job_id,
                resource_path=None,
                is_model=True,
                is_aggregation=True,
            )

        raise ValueError(f"Unsupported artifact={self.job_id}")
