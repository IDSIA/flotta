from typing import Any

from ferdelance.schemas.plans.plan import GenericPlan, GenericStep


class Artifact(GenericPlan):
    def __init__(
        self,
        id: str,
        project_id: str,
        *steps: GenericStep,
        random_seed: Any = None,
    ) -> None:
        super().__init__(Artifact.__name__, *steps, random_seed)

        self.id: str = id
        self.project_id: str = project_id

    def params(self) -> dict[str, Any]:
        return super().params() | {
            "id": self.id,
            "project_id": self.project_id,
        }
