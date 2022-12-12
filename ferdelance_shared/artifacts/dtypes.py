from enum import Enum, auto


class DataType(Enum):
    """Mapping of pandas data types.

    - BOOLEAN: `bool`
    - CATEGORICAL: `category`
    - DATETIME: `datetime64` or `timedelta[ns]`
    - NUMERIC: `int64` or `float64`
    - STRING: `object`
    """
    BOOLEAN = auto()
    CATEGORICAL = auto()
    DATETIME = auto()
    NUMERIC = auto()
    STRING = auto()
