from ferdelance_shared.schemas import Feature as BaseFeature
from ferdelance_shared.operations import NumericOperations, TimeOperations, ObjectOperations

from .queries import QueryFilter, QueryFeature


from datetime import datetime


class Feature(BaseFeature):

    def _filter(self, operation: NumericOperations | TimeOperations | ObjectOperations, value):
        return QueryFilter(
            feature=QueryFeature(feature_id=self.feature_id, datasource_id=self.datasource_id),
            operation=operation.name,
            parameter=f'{value}',
        )

    def __lt__(self, other) -> QueryFilter:
        if isinstance(other, int) or isinstance(other, float):
            return self._filter(NumericOperations.LESS_THAN, other)

        if isinstance(other, datetime):
            return self._filter(TimeOperations.BEFORE)

        raise ValueError('operator less than "<" can be used only for int, float, or time values')

    def __le__(self, other) -> QueryFilter:
        if isinstance(other, int) or isinstance(other, float):
            return self._filter(NumericOperations.LESS_EQUAL, other)

        raise ValueError('operator less equal "<=" can be used only for int or float values')

    def __gt__(self, other) -> QueryFilter:
        if isinstance(other, int) or isinstance(other, float):
            return self._filter(NumericOperations.GREATER_THAN, other)

        if isinstance(other, datetime):
            return self._filter(TimeOperations.AFTER)

        raise ValueError('operator greater than ">" can be used only for int, float, or time values')

    def __ge__(self, other) -> QueryFilter:
        if isinstance(other, int) or isinstance(other, float):
            return self._filter(NumericOperations.GREATER_EQUAL, other)

        raise ValueError('operator greater equal ">=" can be used only for int or float values')

    def __eq__(self, other) -> QueryFilter:
        if isinstance(other, int) or isinstance(other, float):
            return self._filter(NumericOperations.EQUALS, other)

        if isinstance(other, str):
            return self._filter(ObjectOperations.LIKE)

        if isinstance(other, datetime):
            return self._filter(TimeOperations.EQUALS)

        raise ValueError('operator equals "==" can be used only for int, float, str, or time values')

    def __ne__(self, other) -> QueryFilter:
        if isinstance(other, int) or isinstance(other, float):
            return self._filter(NumericOperations.NOT_EQUALS, other)

        if isinstance(other, str):
            return self._filter(ObjectOperations.NOT_LIKE)

        if isinstance(other, datetime):
            return self._filter(TimeOperations.NOT_EQUALS)

        raise ValueError('operator not equals "!=" can be used only for int, float, str, or time values')
