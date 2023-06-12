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
    params = dict()

    for v in signature(c).parameters:
        if v == "local_plan" and plan.local_plan is not None:
            params["local_plan"] = rebuild_plan(plan.local_plan)
        else:
            params[v] = p[v]

    return c(**params)
