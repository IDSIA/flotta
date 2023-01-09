from enum import Enum, auto


class Operations(Enum):
    NUM_LESS_THAN = auto()
    NUM_LESS_EQUAL = auto()
    NUM_GREATER_THAN = auto()
    NUM_GREATER_EQUAL = auto()
    NUM_EQUALS = auto()
    NUM_NOT_EQUALS = auto()

    OBJ_LIKE = auto()
    OBJ_NOT_LIKE = auto()

    TIME_BEFORE = auto()
    TIME_AFTER = auto()
    TIME_EQUALS = auto()
    TIME_NOT_EQUALS = auto()
