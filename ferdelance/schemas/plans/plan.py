from __future__ import annotations
from abc import ABC, abstractmethod
from typing import Any

from ferdelance.schemas.models import GenericModel, Metrics

from pydantic import BaseModel

import pandas as pd


class Plan(BaseModel):
    name: str
    params: dict[str, Any]
    plan: Plan | None = None


class GenericPlan(ABC):
    def __init__(self, name: str, random_seed: Any = None) -> None:
        self.name: str = name
        self.random_seed: Any = random_seed

    def params(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "random_seed": self.random_seed,
        }

    def build(self) -> Plan:
        """Converts the GenericPlan instance to a Plan exchange object.

        Returns:
            Plan:
                Object that can be sent to a server or a client in JSON format.
        """
        return Plan(
            name=self.name,
            params=self.params(),
        )

    @abstractmethod
    def run(self, df: pd.DataFrame, local_model: GenericModel, working_folder: str, artifact_id: str) -> list[Metrics]:
        """Method executed by each client. Implement this method to specify what a client need to do to build and
        evaluate a local model.

        Args:
            df (pd.DataFrame):
                Data to work on from the previous extraction query.
            local_model (GenericModel):
                Description of the local model to build.
            working_folder (str):
                Working folder to use
            artifact_id (str):
                Id of the artifact that will be executed

        Raises:
            NotImplementedError:
                If the plan does not implement this method.
        """
        raise NotImplementedError()
