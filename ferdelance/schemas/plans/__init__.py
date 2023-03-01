__all__ = [
    "rebuild_plan",
    "Plan",
    "TrainTestSplit",
    "TrainTestValSplit",
]

from .loading import Plan, GenericPlan
from .splits import (
    TrainTestSplit,
    TrainTestValSplit,
)

from inspect import signature


def rebuild_plan(plan: Plan) -> GenericPlan:
    c = globals()[plan.name]

    p = plan.params
    params = {v: p[v] for v in signature(c).parameters}

    return c(**params)
