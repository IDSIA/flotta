__all__ = [
    "apply_transformer",
    "save",
    "load",
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
    save,
    load,
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

import pandas as pd
import logging

LOGGER = logging.getLogger(__name__)


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


def apply_transformer(query_transformer: QueryTransformer, df: pd.DataFrame) -> pd.DataFrame:

    if query_transformer.name == "FederatedPipeline":
        transformer = rebuild_pipeline(query_transformer)

    else:
        transformer = rebuild_transformer(query_transformer)

    return transformer.transform(df)