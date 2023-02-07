from ..models import Model

from pydantic import BaseModel


class ArtifactStatus(BaseModel):
    """Details on the artifact status."""

    artifact_id: str | None
    status: str | None


class Artifact(BaseModel):
    """Artifact created in the workbench."""

    artifact_id: str | None = None
    data: None  # TODO
    label: str
    model: Model
    how: None  # TODO
