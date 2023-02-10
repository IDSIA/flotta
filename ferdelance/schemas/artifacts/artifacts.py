from ferdelance.schemas.artifacts.queries import Query
from ferdelance.schemas.models import Model
from ferdelance.schemas.plans import ExecutionPlan

from pydantic import BaseModel


class ArtifactStatus(BaseModel):
    """Details on the artifact status."""

    artifact_id: str | None
    status: str | None


class Artifact(BaseModel):
    """Artifact created in the workbench."""

    artifact_id: str | None = None
    data: list[Query]
    label: str | None = None
    model: Model
    how: ExecutionPlan
