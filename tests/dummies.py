from typing import Any, Sequence

from ferdelance.core.environment import Environment
from ferdelance.core.interfaces import SchedulerContext, SchedulerJob, BaseStep
from ferdelance.core.models import AggregationModel
from ferdelance.core.operations.core import Operation
from ferdelance.core.transformers.core import QueryTransformer
from ferdelance.logging import get_logger

from numpy.typing import ArrayLike

import numpy as np

LOGGER = get_logger(__name__)


class DummyOp(Operation):
    def exec(self, env: Environment) -> Environment:
        return env


class DummyStep(BaseStep):
    def step(self, env: Environment) -> Environment:
        return env

    def jobs(self, context: SchedulerContext) -> Sequence[SchedulerJob]:
        return []

    def bind(self, jobs0: Sequence[SchedulerJob], jobs1: Sequence[SchedulerJob]) -> None:
        return None


class DummyModel(AggregationModel):
    def train(self, x, y) -> Any:
        LOGGER.info("training...")
        return None

    def aggregate(self, model_a, model_b) -> Any:
        LOGGER.info("aggregating...")
        return None

    def predict(self, x) -> np.ndarray:
        return np.zeros(x.shape)

    def classify(self, x) -> ArrayLike | np.ndarray:
        return np.zeros(x.shape)


class DummyTransformer(QueryTransformer):
    def transform(self, env: Environment) -> tuple[Environment, Any]:
        return env, None

    def aggregate(self, env: Environment) -> Environment:
        return env
