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


import pandas as pd
import logging

LOGGER = logging.getLogger(__name__)


def apply_transformer(query_transformer: QueryTransformer, df: pd.DataFrame) -> pd.DataFrame:

    # TODO: this need testing

    c = globals()[query_transformer.name]

    transformer: Transformer = c(query_transformer.parameters)

    return transformer.transform(df)
