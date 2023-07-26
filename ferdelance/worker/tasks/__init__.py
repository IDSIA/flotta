"""This module contains all tasks that can be delegated to a Celery worker."""

__all__ = [
    "aggregation",
    "estimation",
    "training",
]

from .aggregate import aggregation
from .estimate import estimation
from .train import training
