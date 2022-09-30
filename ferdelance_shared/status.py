from enum import Enum, auto


class JobStatus(Enum):
    CREATED = auto()
    SCHEDULED = auto()
    RUNNING = auto()
    COMPLETED = auto()
    ERROR = auto()


class ArtifactJobStatus(Enum):
    SCHEDULED = auto()
    TRAINING = auto()
    AGGREGATING = auto()
    EVALUATING = auto()
    COMPLETED = auto()
    ERROR = auto()
