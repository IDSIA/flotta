from ferdelance.core.artifacts import ArtifactStatus
from ferdelance.shared.status import ArtifactJobStatus

from datetime import datetime
from pydantic import BaseModel
from pathlib import Path


class ServerArtifact(BaseModel):
    """Artifact stored in the database."""

    id: str
    path: Path
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
    component_id: str
    creation_time: datetime | None
    path: Path
    is_external: bool
    is_error: bool


class ResourceIdentifier(BaseModel):
    artifact_id: str
    job_id: str
    iteration: int


class ResourceUse(Resource):
    use: list[ResourceIdentifier] = list()
