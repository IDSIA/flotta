from typing import Any

from ferdelance.config.config import DataSourceConfiguration, DataSourceStorage
from ferdelance.logging import get_logger
from ferdelance.tasks.jobs.generic import GenericJob


LOGGER = get_logger(__name__)


class LocalJob(GenericJob):
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
        super().__init__(component_id, artifact_id, job_id, server_url, private_key, server_public_key)

        self.workdir: str = workdir

        self.datasources: list[DataSourceConfiguration] = [DataSourceConfiguration(**d) for d in datasources]

        self.data = DataSourceStorage(self.datasources)
