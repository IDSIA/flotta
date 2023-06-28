from ferdelance.schemas.artifacts import ArtifactStatus

from datetime import datetime
from pydantic import BaseModel


class ServerArtifact(BaseModel):
    """Artifact stored in the database."""

    id: str
    path: str
    status: str
    creation_time: datetime
    is_model: bool
    is_estimation: bool

    iteration: int

    def get_status(self) -> ArtifactStatus:
        return ArtifactStatus(
            id=self.id,
            status=self.status,
        )


class Result(BaseModel):
    """Model, estimation, or aggregation data stored in the database."""

    id: str
    job_id: str
    artifact_id: str
    client_id: str
    creation_time: datetime | None
    path: str
    is_model: bool = False
    is_estimation: bool = False
    is_aggregation: bool = False
