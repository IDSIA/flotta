from ferdelance.schemas.queries import Query
from ferdelance.schemas.models import Model, GenericModel, rebuild_model
from ferdelance.schemas.plans import Plan, FusionPlan, rebuild_plan
from ferdelance.schemas.estimators import Estimator, GenericEstimator, rebuild_estimator

from pydantic import BaseModel


class ArtifactStatus(BaseModel):
    """Details on the artifact status."""

    id: str
    status: str | None
    resource: str | None = None
    iteration: int = 0


class Artifact(BaseModel):
    """Artifact created in the workbench."""

    id: str = ""
    project_id: str
    transform: Query
    plan: Plan | None = None
    model: Model | None = None
    estimator: Estimator | None = None

    # extra resource to use, must concorde with model/estimator type
    resource_id: str | None = None

    def is_estimation(self):
        return self.estimator is not None

    def is_model(self):
        return self.model is not None

    def has_plan(self) -> bool:
        return self.plan is not None

    def get_plan(self) -> FusionPlan:
        if self.plan is None:
            raise ValueError(f"No plan available with artifact={self.id}")

        plan = rebuild_plan(self.plan)

        if not isinstance(plan, FusionPlan):
            raise ValueError(f"An artifact's plan must be of type FusionPlan, for artifact={self.id}")

        return plan

    def get_model(self) -> GenericModel:
        if self.model is None:
            raise ValueError(f"No model available with artifact={self.id}")

        return rebuild_model(self.model)

    def get_strategy(self) -> str:
        if self.model is None:
            raise ValueError(f"No model available with artifact={self.id}")

        return self.model.strategy

    def get_estimator(self) -> GenericEstimator:
        if self.estimator is None:
            raise ValueError(f"No estimator available with artifact={self.id}")

        return rebuild_estimator(self.estimator)
