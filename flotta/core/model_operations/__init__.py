"""Local plans are executed by each node. The objective of a plan is instruct a
node on how to train and evaluate a model."""

__all__ = [
    "ModelOperation",
    "Train",
    "TrainTest",
    "Aggregation",
    "LocalCrossValidation",
]


from .core import TModelOperation as ModelOperation
from .train import Train, TrainTest
from .aggregations import Aggregation
from .cross_validation import LocalCrossValidation
