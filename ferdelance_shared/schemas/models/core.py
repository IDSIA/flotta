from enum import Enum
from pydantic import BaseModel

import numpy as np


class Strategy(str, Enum):
    """Aggregation strategy selected int the workbench."""
    pass


class Model(BaseModel):
    """Model selected int the workbench."""
    name: str
    strategy: Strategy | None = None

    class Config:
        fields = {
            'model': {'exclude': True},
        }

    def load(self, path) -> None:
        raise NotImplementedError()

    def save(self, path) -> None:
        raise NotImplementedError()

    def predict(self, x: np.ndarray) -> np.ndarray:
        raise NotImplementedError()
