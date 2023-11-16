from ferdelance.core.artifacts import ArtifactStatus
from ferdelance.shared.status import ArtifactJobStatus

from datetime import datetime
from pydantic import BaseModel


class ServerArtifact(BaseModel):
    """Artifact stored in the database."""

    id: str
    path: str
    status: ArtifactJobStatus
    creation_time: datetime

    iteration: int

    def get_status(self) -> ArtifactStatus:
        return ArtifactStatus(
            id=self.id,
            status=self.status,
        )


class Resource(BaseModel):
    """Model, estimation, or aggregation data stored in the database."""

    id: str
    artifact_id: str
    iteration: int
    job_id: str
    component_id: str
    creation_time: datetime | None
    path: str
    is_error: bool
    is_ready: bool
