from ferdelance.schemas.artifacts import ArtifactStatus

from datetime import datetime
from pydantic import BaseModel


class ServerArtifact(BaseModel):
    """Artifact stored in the database."""

    artifact_id: str
    path: str
    status: str
    creation_time: datetime

    def get_status(self) -> ArtifactStatus:
        return ArtifactStatus(
            artifact_id=self.artifact_id,
            status=self.status,
        )


class ServerModel(BaseModel):
    """Model data stored in the database."""

    model_id: str
    creation_time: datetime | None
    path: str
    aggregated: bool = False
    artifact_id: str
    client_id: str
