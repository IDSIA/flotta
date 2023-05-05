__all__ = [
    "rebuild_plan",
    "Plan",
    "TrainAll",
    "TrainTestSplit",
    "TrainTestValSplit",
    "IterativePlan",
]

from .core import Plan, GenericPlan
from .splits import (
    TrainAll,
    TrainTestSplit,
    TrainTestValSplit,
)
from .iterative import (
    IterativePlan,
)

from inspect import signature


def rebuild_plan(plan: Plan) -> GenericPlan:
    c = globals()[plan.name]

    p = plan.params
    params = {v: p[v] for v in signature(c).parameters}

    if plan.local_plan is not None:
        params["local_plan"] = rebuild_plan(plan.local_plan)

    return c(**params)
