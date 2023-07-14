from typing import Protocol


class Action(Protocol):
    def execute(self) -> None:
        ...
