from pydantic import BaseModel

import numpy as np


class Strategy(BaseModel):
    """Aggregation strategy selected int the workbench."""
    strategy: str


class Model(BaseModel):
    """Model selected int the workbench."""
    name: str
    strategy: Strategy

    def load(self, path) -> None:
        raise NotImplementedError()

    def save(self, path) -> None:
        raise NotImplementedError()

    def predict(self, x: np.ndarray) -> np.ndarray:
        raise NotImplementedError()
