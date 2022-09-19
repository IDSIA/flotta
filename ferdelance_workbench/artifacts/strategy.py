from typing import Any


class Strategy:

    def __init__(self, strategy: str) -> None:
        self.strategy = strategy

    def json(self) -> dict[str, Any]:
        return {
            'strategy': self.strategy,
        }

    def __str__(self) -> str:
        return f'Strategy: {self.strategy}'

    def __repr__(self) -> str:
        return self.__str__()
