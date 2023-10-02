from typing import Any

from ferdelance.config.config import DataSourceStorage
from ferdelance.logging import get_logger
from ferdelance.schemas.artifacts import Artifact
from ferdelance.schemas.estimators import apply_estimator
from ferdelance.schemas.tasks import TaskParameters, TaskResult, TaskError
from ferdelance.tasks.jobs.commons import setup, apply_transform
from ferdelance.tasks.jobs.local import LocalJob

import ray


LOGGER = get_logger(__name__)


def run_estimate(data: DataSourceStorage, task: TaskParameters) -> TaskResult:
    job_id = task.job_id
    artifact: Artifact = task.artifact
    artifact.id = artifact.id

    working_folder = setup(artifact, job_id, task.iteration)

    df_dataset = apply_transform(artifact, task, data, working_folder)

    if artifact.estimator is None:
        raise ValueError("Artifact is not an estimation!")  # TODO: manage this!

    LOGGER.info(f"artifact={artifact.id}: executing estimation")

    path_estimator = apply_estimator(artifact.estimator, df_dataset, working_folder, artifact.id)

    return TaskResult(
        job_id=job_id,
        result_path=path_estimator,
        is_estimate=True,
    )


@ray.remote
class EstimationJob(LocalJob):
    def __init__(
        self,
        component_id: str,
        artifact_id: str,
        job_id: str,
        server_url: str,
        private_key: str,
        server_public_key: str,
        workdir: str,
        datasources: list[dict[str, Any]],
    ) -> None:
        super().__init__(
            component_id, artifact_id, job_id, server_url, private_key, server_public_key, workdir, datasources
        )

    def __repr__(self) -> str:
        return f"Estimation{super().__repr__()}"

    def run(self):
        task: TaskParameters = self.routes_service.get_task_params(self.artifact_id, self.job_id)

        if task.artifact.is_estimation():
            res = self.estimate(task)

            self.routes_service.post_result(self.artifact_id, self.job_id, path_in=res.result_path)

        else:
            self.routes_service.post_error(
                task.job_id,
                task.artifact.id,
                TaskError(
                    job_id=task.job_id,
                    message="Malformed artifact",
                ),
            )

    def estimate(self, task: TaskParameters) -> TaskResult:
        res: TaskResult = run_estimate(self.data, task)
        return res
