from typing import Any

from abc import ABC, abstractmethod


class Distribution(ABC):
    @abstractmethod
    def distribute(self, env: dict[str, Any]) -> None:
        raise NotImplementedError()


class Distribute(Distribution):
    def distribute(self, env: dict[str, Any]) -> None:
        return super().distribute(env)


class Collect(Distribution):
    def distribute(self, env: dict[str, Any]) -> None:
        return super().distribute(env)
