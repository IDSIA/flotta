from abc import ABC, abstractmethod

from ferdelance.logging import get_logger
from ferdelance.tasks.services import EncryptRouteService, RouteService


LOGGER = get_logger(__name__)


class GenericJob(ABC):
    def __init__(
        self,
        component_id: str,
        artifact_id: str,
        job_id: str,
        node_url: str,
        private_key: str,
        node_public_key: str,
    ) -> None:
        self.component_id: str = component_id
        self.artifact_id: str = artifact_id
        self.job_id: str = job_id

        self.routes_service: RouteService = EncryptRouteService(
            self.component_id,
            node_url,
            private_key,
            node_public_key,
        )

    def __repr__(self) -> str:
        return f"Job artifact={self.artifact_id} job={self.job_id}"

    @abstractmethod
    def run(self):
        raise NotImplementedError()
