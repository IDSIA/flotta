from typing import Any

from ferdelance.logging import get_logger
from ferdelance.schemas.artifacts import Artifact
from ferdelance.schemas.tasks import TaskDone, TaskError, TaskParameters
from ferdelance.tasks.jobs.commons import setup, apply_transform
from ferdelance.tasks.jobs.local import LocalJob

import ray


LOGGER = get_logger(__name__)


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
        datasources: list[dict[str, Any]],
    ) -> None:
        super().__init__(component_id, artifact_id, job_id, node_url, private_key, node_public_key, datasources)

    def __repr__(self) -> str:
        return f"Training{super().__repr__()}"

    def run(self):
        context: TaskParameters = self.routes_service.get_params(artifact_id=self.artifact_id, job_id=self.job_id)

        try:
            job_id = context.job_id
            artifact: Artifact = context.artifact

            if not artifact.is_model():
                raise ValueError("Artifact is not a model!")

            if artifact.model is not None and artifact.plan is None:
                raise ValueError("Invalid artifact training")

            working_folder = setup(artifact, job_id, context.iteration)

            datasource_hashes = context.data["datasource_hashes"]

            df_data = apply_transform(artifact, datasource_hashes, self.data, working_folder)

            LOGGER.info(f"artifact={artifact.id}: executing model training")

            # model preparation
            local_model = artifact.get_model()

            # load execution plan
            plan = artifact.get_plan()

            plan_res = plan.run(df_data, local_model, working_folder, artifact.id)

            if plan_res.path is None:
                raise ValueError("Model path not set!")  # TODO: manage this!

            task_res = TaskDone(
                job_id=job_id,
                resource_path=plan_res.path,
                metrics=plan_res.metrics,
                is_model=True,
            )

            for m in plan_res.metrics:
                m.job_id = task_res.job_id
                self.routes_service.post_metrics(self.artifact_id, self.job_id, m)

            self.routes_service.post_result(self.artifact_id, self.job_id, path_in=task_res.resource_path)

        except Exception as e:
            LOGGER.error(e)
            LOGGER.exception(e)
            self.routes_service.post_error(
                context.job_id,
                context.artifact.id,
                TaskError(
                    job_id=context.job_id,
                    message=f"Malformed artifact: {e}",
                ),
            )
