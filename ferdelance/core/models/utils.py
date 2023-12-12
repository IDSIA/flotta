from typing import Any, Callable

import inspect


def get_model_parameters(create_function: Callable, in_params: dict[str, Any]) -> dict[str, Any]:
    sig = inspect.signature(create_function)

    sig_params = sig.parameters
    params = {p: in_params.get(p, sig_params[p].default) for p in sig_params.keys()}

    return params
