from ferdelance.config.config import config_manager
from ferdelance.logging import get_logger
from ferdelance.schemas.artifacts import Artifact
from ferdelance.schemas.estimators import save_estimator
from ferdelance.schemas.tasks import TaskError, TaskParameters
from ferdelance.tasks.jobs.commons import setup
from ferdelance.tasks.jobs.local import GenericJob

import os
import ray


LOGGER = get_logger(__name__)


@ray.remote
class InitializationJob(GenericJob):
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

    def run(self):
        context: TaskParameters = self.routes_service.get_params(
            self.artifact_id,
            self.job_id,
        )
        artifact: Artifact = context.artifact

        working_folder = setup(artifact, self.job_id, context.iteration)

        if artifact.is_model():
            if artifact.resource_id is None:
                # initialize empty model
                model = artifact.get_model()
                model.initialize()

                path: str = config_manager.get().store_resource(
                    artifact.id,
                    self.job_id,
                    context.iteration,
                    False,
                    True,
                    artifact.is_model(),
                    artifact.is_estimation(),
                )

                model.save(path)

                self.routes_service.post_result(
                    self.artifact_id,
                    self.job_id,
                    None,  # should be local
                )

        elif artifact.is_estimation():
            estimator = artifact.get_estimator()
            estimator.initialize()

            path_estimator = os.path.join(working_folder, f"{artifact.id}_Estimator_{estimator.name}.pkl")

            save_estimator(estimator, path_estimator)

            self.routes_service.post_result(
                self.artifact_id,
                self.job_id,
                None,  # should be local
            )

        else:
            self.routes_service.post_error(
                artifact.id,
                self.job_id,
                TaskError(
                    job_id=self.job_id,
                    message="Invalid artifact: no model or estimator defined",
                ),
            )
            return
