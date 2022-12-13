__all__ = [
    'apply_transformer',
    'save',
    'load',
    'Transformer',

    'FederatedPipeline',

    'FederatedMinMaxScaler',
    'FederatedStandardScaler',

    'FederatedKBinsDiscretizer',
    'FederatedBinarizer',
    'FederatedLabelBinarizer',
    'FederatedOneHotEncoder',

    'FederatedSimpleImputer',

    'FederatedDrop',
    'FederatedRename',
]

from typing import Any
from ferdelance_shared.artifacts import QueryTransformer

from .core import (
    Transformer,
    save,
    load,
)

from .pipelines import (
    FederatedPipeline,
)
from .scaling import (
    FederatedMinMaxScaler,
    FederatedStandardScaler,
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


def rebuild_transformer(query_transformer: QueryTransformer) -> Transformer:
    c = globals()[query_transformer.name]

    p = query_transformer.params()
    params = {v: p[v] for v in signature(c).parameters}

    return c(**params)


def rebuild_pipeline(query_transformer: QueryTransformer) -> FederatedPipeline:

    stages = []

    params = query_transformer.params()

    for stage in params['stages']:
        qt = QueryTransformer(**stage)

        if stage['name'] == 'FederatedPipeline':
            transformer = rebuild_pipeline(qt)
        else:
            transformer = rebuild_transformer(qt)

        stages.append(transformer)

    return FederatedPipeline(stages)


def apply_transformer(query_transformer: QueryTransformer, df: pd.DataFrame) -> pd.DataFrame:

    if query_transformer.name == 'FederatedPipeline':
        transformer = rebuild_pipeline(query_transformer)

    else:
        transformer = rebuild_transformer(query_transformer)

    return transformer.transform(df)
