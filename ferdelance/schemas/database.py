from ferdelance.schemas.artifacts import ArtifactStatus

from datetime import datetime
from pydantic import BaseModel


class ServerArtifact(BaseModel):
    """Artifact stored in the database."""

    artifact_id: str
    path: str
    status: str
    creation_time: datetime
    is_model: bool
    is_estimation: bool

    def get_status(self) -> ArtifactStatus:
        return ArtifactStatus(
            artifact_id=self.artifact_id,
            status=self.status,
        )


class Result(BaseModel):
    """Model, estimation, or aggregation data stored in the database."""

    job_id: str
    result_id: str
    artifact_id: str
    client_id: str
    creation_time: datetime | None
    path: str
    is_model: bool = False
    is_estimation: bool = False
    is_aggregation: bool = False
