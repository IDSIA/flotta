from ..artifacts import (
    QueryFilter,
)
from .otypes import Operations

import pandas as pd
import logging

LOGGER = logging.getLogger(__name__)


def apply_filter(query_filter: QueryFilter, df: pd.DataFrame) -> pd.DataFrame:

    feature: str = query_filter.feature.feature_name
    operation: Operations = Operations[query_filter.operation]
    parameter: str = query_filter.parameter

    LOGGER.info(f"Applying {operation}({parameter}) on {feature}")

    if operation == Operations.NUM_LESS_THAN:
        return df[df[feature] < float(parameter)]
    if operation == Operations.NUM_LESS_EQUAL:
        return df[df[feature] <= float(parameter)]
    if operation == Operations.NUM_GREATER_THAN:
        return df[df[feature] > float(parameter)]
    if operation == Operations.NUM_GREATER_EQUAL:
        return df[df[feature] >= float(parameter)]
    if operation == Operations.NUM_EQUALS:
        return df[df[feature] == float(parameter)]
    if operation == Operations.NUM_NOT_EQUALS:
        return df[df[feature] != float(parameter)]

    if operation == Operations.OBJ_LIKE:
        return df[df[feature] == parameter]
    if operation == Operations.OBJ_NOT_LIKE:
        return df[df[feature] != parameter]

    if operation == Operations.TIME_BEFORE:
        return df[df[feature] < pd.to_datetime(parameter)]
    if operation == Operations.TIME_AFTER:
        return df[df[feature] > pd.to_datetime(parameter)]
    if operation == Operations.TIME_EQUALS:
        return df[df[feature] == pd.to_datetime(parameter)]
    if operation == Operations.TIME_NOT_EQUALS:
        return df[df[feature] != pd.to_datetime(parameter)]
