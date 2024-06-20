from typing import Sequence

from ferdelance.core.distributions import Collect
from ferdelance.core.interfaces import Step
from ferdelance.core.model import Model
from ferdelance.core.model_operations import Aggregation, Train
from ferdelance.core.steps import Finalize, Parallel


class AggregationModel(Model):
    def get_steps(self) -> Sequence[Step]:
        return [
            Parallel(
                Train(
                    query=self.query,
                    model=self,
                ),
                Collect(),
            ),
            Finalize(
                Aggregation(
                    model=self,
                ),
            ),
        ]
