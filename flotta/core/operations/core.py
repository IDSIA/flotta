from abc import abstractmethod

from flotta.core.entity import Entity
from flotta.core.environment import Environment
from flotta.core.queries import Query

from pydantic import SerializeAsAny


class Operation(Entity):
    data_names: list[str] = list()  # variables from local data
    env_names: list[str] = list()  # variables in the environment

    @abstractmethod
    def exec(self, env: Environment) -> Environment:
        raise NotImplementedError()


class QueryOperation(Operation):
    query: Query | None = None


class DoNothing(Operation):
    def exec(self, env: Environment) -> Environment:
        return env


TOperation = SerializeAsAny[Operation]
