from typing import Any
from pydantic import BaseModel

import numpy as np


class Model(BaseModel):
    """Exchange model description defined in the workbench, trained in 
    the clients, and aggregated in the server.
    """
    name: str
    strategy: str | None = None
    parameters: dict[str, Any]


class GenericModel:
    """This is the class that can manipulate real models."""

    def load(self, path) -> None:
        raise NotImplementedError()

    def save(self, path) -> None:
        raise NotImplementedError()

    def predict(self, x: np.ndarray) -> np.ndarray:
        raise NotImplementedError()

    def build(self) -> Model:
        raise NotImplementedError()
