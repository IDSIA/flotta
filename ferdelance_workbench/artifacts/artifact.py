from ferdelance_shared.schemas import Artifact as BaseArtifact
from .dataset import Dataset


class Artifact(BaseArtifact):
    dataset: Dataset
    artifact_id: str | None = None
