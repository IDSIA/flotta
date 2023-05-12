__all__ = [
    "apply_transformer",
    "save",
    "run",
    "Transformer",
    "FederatedPipeline",
    "FederatedFilter",
    "FederatedMinMaxScaler",
    "FederatedStandardScaler",
    "FederatedKBinsDiscretizer",
    "FederatedBinarizer",
    "FederatedLabelBinarizer",
    "FederatedOneHotEncoder",
    "FederatedClamp",
    "FederatedSimpleImputer",
    "FederatedDrop",
    "FederatedRename",
]

from ferdelance.schemas.queries import QueryTransformer

from .core import (
    Transformer,
)
from .filters import (
    FederatedFilter,
)
from .pipelines import (
    FederatedPipeline,
)
from .scaling import (
    FederatedMinMaxScaler,
    FederatedStandardScaler,
    FederatedClamp,
)
from .discrete import (
    FederatedKBinsDiscretizer,
    FederatedBinarizer,
    FederatedLabelBinarizer,
    FederatedOneHotEncoder,
)
from .imputation import (
    FederatedSimpleImputer,
)
from .utils import (
    FederatedDrop,
    FederatedRename,
)

from inspect import signature

import os
import pickle
import pandas as pd
import logging

LOGGER = logging.getLogger(__name__)


def save(obj: Transformer, path: str) -> None:
    with open(path, "wb") as f:
        pickle.dump(obj, f)


def run(path: str) -> Transformer:
    with open(path, "rb") as f:
        return pickle.load(f)


def rebuild_transformer(transformer: QueryTransformer) -> Transformer:
    LOGGER.info(f"apply transformer {transformer.name}")
    c = globals()[transformer.name]

    p = transformer.params()
    params = {v: p[v] for v in signature(c).parameters}

    return c(**params)


def rebuild_pipeline(query_transformer: QueryTransformer) -> FederatedPipeline:
    stages = []

    params = query_transformer.params()

    for stage in params["stages"]:
        qt = QueryTransformer(**stage)

        if stage["name"] == "FederatedPipeline":
            transformer = rebuild_pipeline(qt)
        else:
            transformer = rebuild_transformer(qt)

        stages.append(transformer)

    return FederatedPipeline(stages)


def apply_transformer(
    query_transformer: QueryTransformer,
    df: pd.DataFrame,
    working_folder: str | None = None,
    artifact_id: str | None = None,
    i: int | None = None,
) -> pd.DataFrame:
    if query_transformer.name == "FederatedPipeline":
        transformer = rebuild_pipeline(query_transformer)

    else:
        transformer = rebuild_transformer(query_transformer)

    if working_folder is not None and artifact_id is not None and i is not None:
        path_transformer = os.path.join(working_folder, f"{artifact_id}_{i:04}_Transformer_{transformer.name}.pkl")

        save(transformer, path_transformer)

    return transformer.transform(df)
