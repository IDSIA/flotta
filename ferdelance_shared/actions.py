from enum import Enum, auto


class Action(Enum):
    """Execute a new train query."""
    EXECUTE = auto()

    """Server send that the token is expired, update with a new one."""
    UPDATE_TOKEN = auto()
    """Server send that there is a new client version, update it."""
    UPDATE_CLIENT = auto()
    """Server send that it changed key, update it."""
    UPDATE_SERVER_KEY = auto()

    """No action required."""
    DO_NOTHING = auto()

    """Client self-update."""
    CLIENT_UPDATE = auto()
    """Client can terminate application."""
    CLIENT_EXIT = auto()
