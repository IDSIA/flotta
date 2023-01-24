from enum import Enum, auto


class Action(Enum):
    """Enums to describe actions done by the client and issued by the server"""

    """Initialize client"""
    INIT = auto()

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

    """Component self-update."""
    CLIENT_UPDATE = auto()
    """Component can terminate application."""
    CLIENT_EXIT = auto()
