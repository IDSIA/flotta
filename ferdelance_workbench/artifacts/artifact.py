from ferdelance_shared.schemas import Artifact as BaseArtifact
from .queries import Query


class Artifact(BaseArtifact):
    queries: list[Query]
