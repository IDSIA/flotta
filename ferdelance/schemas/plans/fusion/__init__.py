"""Fusion plans instruct the framework on how to merge together the model trained
on local data."""

__all__ = [
    "FusionPlan",
    "IterativePlan",
    "SequencePlan",
    "ParallelPlan",
]

from .core import FusionPlan
from .iterative import IterativePlan
from .sequence import SequencePlan
from .parallel import ParallelPlan
