__all__ = [
    "rebuild_plan",
    "Plan",
    "GenericPlan",
    "TrainAll",
    "TrainTestSplit",
    "LocalCrossValidation",
    "IterativePlan",
    "SequencePlan",
    "IterativePlan",
    "ParallelPlan",
]

from .plan import GenericPlan, Plan

from .local import (
    LocalPlan,
    TrainAll,
    TrainTestSplit,
    LocalCrossValidation,
)
from .fusion import (
    FusionPlan,
    IterativePlan,
    SequencePlan,
    ParallelPlan,
)

from inspect import signature


def rebuild_plan(plan: Plan) -> LocalPlan | FusionPlan:
    c = globals()[plan.name]

    p = plan.params
    params = dict()

    for v in signature(c).parameters:
        if v == "local_plan" and plan.plan is not None:
            params["local_plan"] = rebuild_plan(plan.plan)
        else:
            params[v] = p[v]

    return c(**params)
