from typing import Sequence

from flotta.core.distributions import Collect
from flotta.core.interfaces import Step
from flotta.core.model import Model
from flotta.core.model_operations import Aggregation, Train
from flotta.core.steps import Finalize, Parallel


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
