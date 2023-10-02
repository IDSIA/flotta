from ferdelance.tasks.jobs.generic import GenericJob

import ray


@ray.remote
class PrepareContextData(GenericJob):
    def __init__(
        self, component_id: str, artifact_id: str, job_id: str, node_url: str, private_key: str, node_public_key: str
    ) -> None:
        super().__init__(component_id, artifact_id, job_id, node_url, private_key, node_public_key)

    def run(self):
        return super().run()
