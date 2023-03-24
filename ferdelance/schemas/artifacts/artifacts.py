from ferdelance.schemas.queries.queries import Query
from ferdelance.schemas.models import Model
from ferdelance.schemas.plans import Plan
from ferdelance.schemas.estimators import Estimator

from pydantic import BaseModel


class ArtifactStatus(BaseModel):
    """Details on the artifact status."""

    artifact_id: str | None
    status: str | None
    results: str | None = None


class Artifact(BaseModel):
    """Artifact created in the workbench."""

    artifact_id: str | None = None
    project_id: str
    transform: Query
    load: Plan | None = None
    model: Model | None = None
    estimate: Estimator | None = None

    def is_estimation(self):
        return self.estimate is not None

    def is_model(self):
        return self.model is not None
