from __future__ import annotations
from typing import Any, Sequence
from abc import ABC, abstractmethod

from ferdelance.core.entity import Entity
from ferdelance.core.interfaces import Step
from ferdelance.core.queries import Query

from pydantic import SerializeAsAny


class Estimator(ABC, Entity):
    query: Query | None = None

    def __init__(
        self,
        query: Query | None = None,
        random_state: Any = None,
        **data,
    ):
        super(Estimator, self).__init__(
            query=query,  # type: ignore
            random_state=random_state,  # type: ignore
            **data,
        )

    @abstractmethod
    def get_steps(self) -> Sequence[Step]:
        raise NotImplementedError()


TEstimator = SerializeAsAny[Estimator]
