from enum import Enum, auto


class JobStatus(Enum):
    WAITING = auto()
    SCHEDULED = auto()
    RUNNING = auto()
    COMPLETED = auto()
    ERROR = auto()


class ArtifactJobStatus(Enum):
    SCHEDULED = auto()
    RUNNING = auto()
    COMPLETED = auto()
    ERROR = auto()
