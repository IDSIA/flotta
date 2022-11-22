from pydantic import BaseModel
from .queries import Dataset
from ..models import Model


class BaseArtifact(BaseModel):
    """Basic structure for artifact"""
    artifact_id: str | None


class Artifact(BaseArtifact):
    """Artifact created in the workbench."""
    dataset: Dataset
    model: Model


class ArtifactStatus(BaseArtifact):
    """Details on the artifact."""
    status: str | None
