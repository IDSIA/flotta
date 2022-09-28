from enum import Enum, auto


class NumericOperations(Enum):
    LESS_THAN = auto()
    LESS_EQUAL = auto()
    GREATER_THAN = auto()
    GREATER_EQUAL = auto()
    EQUALS = auto()
    NOT_EQUALS = auto()


class ObjectOperations(Enum):
    LIKE = auto()
    NOT_LIKE = auto()


class TimeOperations(Enum):
    BEFORE = auto()
    AFTER = auto()
    EQUALS = auto()
    NOT_EQUALS = auto()
