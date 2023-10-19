__all__ = [
    "rebuild_plan",
    "Plan",
    "GenericPlan",
]

from .plan import GenericPlan, Plan

from inspect import signature


def rebuild_plan(plan: Plan) -> GenericPlan:
    c = globals()[plan.name]

    p = plan.params
    params = dict()

    for v in signature(c).parameters:
        if v == "steps" and plan.steps:
            params["steps"] = [rebuild_plan(step) for step in plan.steps]
        else:
            params[v] = p[v]

    return c(**params)
