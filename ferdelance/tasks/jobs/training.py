from typing import Any
from ferdelance.config.config import DataSourceStorage

from ferdelance.logging import get_logger
from ferdelance.schemas.artifacts import Artifact
from ferdelance.schemas.tasks import TaskParameters, TaskResult, TaskError
from ferdelance.tasks.jobs.commons import setup, apply_transform
from ferdelance.tasks.jobs.local import LocalJob

import ray


LOGGER = get_logger(__name__)


def run_training(data: DataSourceStorage, task: TaskParameters) -> TaskResult:
    job_id = task.job_id
    artifact: Artifact = task.artifact

    working_folder = setup(artifact, job_id, task.iteration)

    df_dataset = apply_transform(artifact, task, data, working_folder)

    if artifact.model is not None and artifact.plan is None:
        raise ValueError("Invalid artifact training")  # TODO: manage this!

    LOGGER.info(f"artifact={artifact.id}: executing model training")

    # model preparation
    local_model = artifact.get_model()

    # LOAD execution plan
    plan = artifact.get_plan()

    metrics = plan.run(df_dataset, local_model, working_folder, artifact.id)

    if plan.path_model is None:
        raise ValueError("Model path not set!")  # TODO: manage this!

    return TaskResult(
        job_id=job_id,
        result_path=plan.path_model,
        metrics=metrics,
        is_model=True,
    )


@ray.remote
class TrainingJob(LocalJob):
    def __init__(
        self,
        component_id: str,
        artifact_id: str,
        job_id: str,
        node_url: str,
        private_key: str,
        node_public_key: str,
        workdir: str,
        datasources: list[dict[str, Any]],
    ) -> None:
        super().__init__(
            component_id, artifact_id, job_id, node_url, private_key, node_public_key, workdir, datasources
        )

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

                self.routes_service.post_result(self.artifact_id, self.job_id, path_in=res.result_path)

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

    def train(self, task: TaskParameters) -> TaskResult:
        res: TaskResult = run_training(self.data, task)
        return res
