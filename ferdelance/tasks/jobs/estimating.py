from typing import Any

from ferdelance.logging import get_logger
from ferdelance.core.artifacts import Artifact
from ferdelance.schemas.tasks import TaskDone, TaskError, TaskParameters
from ferdelance.tasks.jobs.commons import setup, apply_transform
from ferdelance.tasks.jobs.local import LocalJob

import os
import ray


LOGGER = get_logger(__name__)


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
        datasources: list[dict[str, Any]],
    ) -> None:
        super().__init__(component_id, artifact_id, job_id, server_url, private_key, server_public_key, datasources)

    def __repr__(self) -> str:
        return f"Estimation{super().__repr__()}"

    def run(self):
        context: TaskParameters = self.routes_service.get_params(self.artifact_id, self.job_id)

        if context.artifact.is_estimation():
            job_id = context.job_id
            artifact: Artifact = context.artifact

            working_folder = setup(artifact, job_id, context.iteration)

            hashes = context.data["datasource_hashes"]

            df_dataset = apply_transform(artifact, hashes, self.data, working_folder)

            LOGGER.info(f"artifact={artifact.id}: executing estimation")

            # TODO: use plan!!!

            local_estimator = artifact.get_estimator()
            local_estimator.fit(df_dataset)

            path_estimator = os.path.join(working_folder, f"{artifact.id}_Estimator_{local_estimator.name}.pkl")

            save_estimator(local_estimator, path_estimator)

            res = TaskDone(
                job_id=job_id,
                resource_path=path_estimator,
                is_estimate=True,
            )

            self.routes_service.post_result(self.artifact_id, self.job_id, path_in=res.resource_path)

        else:
            self.routes_service.post_error(
                context.job_id,
                context.artifact.id,
                TaskError(
                    job_id=context.job_id,
                    message="Malformed artifact",
                ),
            )
