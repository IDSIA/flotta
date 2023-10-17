__all__ = [
    "rebuild_estimator",
    "Estimator",
    "GenericEstimator",
    "CountEstimator",
    "GroupEstimator",
    "GroupCountEstimator",
    "GroupMeanEstimator",
    "MeanEstimator",
]

from .core import Estimator, GenericEstimator
from .counters import CountEstimator
from .means import MeanEstimator
from .groups import GroupEstimator, GroupCountEstimator, GroupMeanEstimator

from inspect import signature

import pickle


def save_estimator(obj: GenericEstimator, path: str) -> None:
    with open(path, "wb") as f:
        pickle.dump(obj, f)


def load_estimator(path: str) -> GenericEstimator:
    with open(path, "rb") as f:
        return pickle.load(f)


def rebuild_estimator(estimator: Estimator) -> GenericEstimator:
    """Rebuilds the estimator given its description. The description came from
    the server in string format, then it is converted in the descriptor, and
    here it can be used to rebuild (o recreate) the original object.

    Args:
        estimator (Estimator):
            Descriptor of the estimator to rebuild.

    Returns:
        GenericEstimator:
            The estimator, in generic form, that can be used.
    """

    c = globals()[estimator.name]

    p = estimator.params

    if p:
        params = {v: p[v] for v in signature(c).parameters}

        if len(estimator.features_in) == 1:
            return c(**params, feature_in=estimator.features_in[0])

        return c(**params, features_in=estimator.features_in)

    if len(estimator.features_in) == 1:
        return c(feature_in=estimator.features_in[0])

    return c(features_in=estimator.features_in)
