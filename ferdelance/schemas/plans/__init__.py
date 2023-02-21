__all__ = [
    "rebuild_plan",
    "LoadingPlan",
    "TrainTestSplit",
    "TrainTestValSplit",
]

from .loading import LoadingPlan, BasePlan
from .splits import (
    TrainTestSplit,
    TrainTestValSplit,
)

from inspect import signature


def rebuild_plan(plan: LoadingPlan) -> BasePlan:
    c = globals()[plan.name]

    p = plan.params
    params = {v: p[v] for v in signature(c).parameters}

    return c(**params)
