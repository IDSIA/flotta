from enum import Enum
from pydantic import BaseModel

import numpy as np


class Strategy(str, Enum):
    """Aggregation strategy selected int the workbench."""
    pass


class Parameters(BaseModel):
    """Class defining all the parameters accepted in training by the model."""
    pass


class Model(BaseModel):
    """Model defined in the workbench and trained in the clients."""
    name: str
    strategy: Strategy | None = None
    parameters: Parameters = Parameters()

    class Config:
        arbitrary_types_allowed = True
        fields = {
            'model': {'exclude': True},
        }

    def load(self, path) -> None:
        raise NotImplementedError()

    def save(self, path) -> None:
        raise NotImplementedError()

    def predict(self, x: np.ndarray) -> np.ndarray:
        raise NotImplementedError()
