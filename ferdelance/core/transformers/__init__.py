__all__ = [
    "apply_transformer",
    "save",
    "run",
    "QueryTransformer",
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

from ferdelance.logging import get_logger

from .core import (
    QueryTransformer,
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

import os
import pickle
import pandas as pd

LOGGER = get_logger(__name__)


def save(obj: Transformer, path: str) -> None:
    with open(path, "wb") as f:
        pickle.dump(obj, f)


def run(path: str) -> Transformer:
    with open(path, "rb") as f:
        return pickle.load(f)


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
