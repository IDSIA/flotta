from enum import Enum, auto


class Action(Enum):
    """Execute a new train query."""
    EXECUTE = auto()

    """Token is expired, update with a new one."""
    UPDATE_TOKEN = auto()
    """There is a new client version, update it."""
    UPDATE_CLIENT = auto()
    """Server changed key, update it."""
    UPDATE_SERVER_KEY = auto()

    """No action required."""
    DO_NOTHING = auto()
