from typing import Any

from ferdelance.schemas.transformers.core import Transformer
from ferdelance.schemas.queries import QueryFeature, Operations

import pandas as pd


class FederatedFilter(Transformer):
    def __init__(self, feature: QueryFeature | str, operation: Operations | str, value) -> None:
        """This is a special case of transformer where a set of feature will not
        be changed but it will be used to reduce the amount of data by the
        application of a filter.
        :param feature:
            Feature to apply the filter to.
        :param op:
            Operation to be performed by the filter.
        :param value:
            Parameter of the filter to be applied to the feature. This can be a
            float, an integer, or a date in string format.
        """
        super().__init__(FederatedFilter.__name__)

        if isinstance(feature, QueryFeature):
            self.feature: str = feature.name
        else:
            self.feature: str = feature

        if isinstance(operation, Operations):
            self.operation: str = operation.name
        else:
            self.operation = operation

        self.value: str = f"{value}"

    def params(self) -> dict[str, Any]:
        return super().params() | {
            "feature": self.feature,
            "operation": self.operation,
            "value": self.value,
        }

    def aggregate(self) -> None:
        # TODO: no aggregation required?
        return super().aggregate()

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        feature: str = self.feature
        op: Operations = Operations[self.operation]
        parameter: str = self.value

        if feature not in df.columns:
            # no change applied
            return df

        if op == Operations.NUM_LESS_THAN:
            return df[df[feature] < float(parameter)]
        if op == Operations.NUM_LESS_EQUAL:
            return df[df[feature] <= float(parameter)]
        if op == Operations.NUM_GREATER_THAN:
            return df[df[feature] > float(parameter)]
        if op == Operations.NUM_GREATER_EQUAL:
            return df[df[feature] >= float(parameter)]
        if op == Operations.NUM_EQUALS:
            return df[df[feature] == float(parameter)]
        if op == Operations.NUM_NOT_EQUALS:
            return df[df[feature] != float(parameter)]

        if op == Operations.OBJ_LIKE:
            return df[df[feature] == parameter]
        if op == Operations.OBJ_NOT_LIKE:
            return df[df[feature] != parameter]

        if op == Operations.TIME_BEFORE:
            return df[df[feature] < pd.to_datetime(parameter)]
        if op == Operations.TIME_AFTER:
            return df[df[feature] > pd.to_datetime(parameter)]
        if op == Operations.TIME_EQUALS:
            return df[df[feature] == pd.to_datetime(parameter)]
        if op == Operations.TIME_NOT_EQUALS:
            return df[df[feature] != pd.to_datetime(parameter)]

        raise ValueError(f'Unsupported operation "{self.operation}" ')
