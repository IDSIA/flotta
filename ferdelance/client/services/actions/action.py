from typing import Protocol


class Action(Protocol):
    def execute(self) -> None:
        ...

    def validate_input(self) -> None:
        ...
