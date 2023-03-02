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

import os
import pandas as pd
import pickle


def save(obj: GenericEstimator, path: str) -> None:
    with open(path, "wb") as f:
        pickle.dump(obj, f)


def load(path: str) -> GenericEstimator:
    with open(path, "rb") as f:
        return pickle.load(f)


def rebuild_estimator(estimator: Estimator) -> GenericEstimator:

    c = globals()[estimator.name]

    p = estimator.params
    params = {v: p[v] for v in signature(c).parameters}

    return c(**params)


def apply_estimator(estimator: Estimator, df: pd.DataFrame, working_folder: str, artifact_id: str) -> str:
    e = rebuild_estimator(estimator)

    e.fit(df)

    path_estimator = os.path.join(working_folder, f"{artifact_id}_Estimator_{estimator.name}.pkl")

    save(e, path_estimator)

    return path_estimator
