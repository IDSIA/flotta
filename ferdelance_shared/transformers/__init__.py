__all__ = [
    'apply_transformer',
    'save',
    'load',
    'Transformer',
    'FederatedPipeline',
    'FederatedMinMaxScaler',
]

from ferdelance_shared.artifacts import QueryTransformer

from .core import (
    save,
    load,
    Transformer,
)

from .pipelines import FederatedPipeline
from .scaling import FederatedMinMaxScaler


import pandas as pd
import logging

LOGGER = logging.getLogger(__name__)


def apply_transformer(query_transformer: QueryTransformer, df: pd.DataFrame) -> pd.DataFrame:

    feature: str = query_transformer.feature.feature_name
    operation: str = query_transformer.name
    parameter: str = query_transformer.parameters

    return df
