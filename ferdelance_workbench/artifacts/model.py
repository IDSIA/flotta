from typing import Any


class Model:

    def __init__(self, name: str) -> None:
        self.name = name

    def json(self) -> dict[str, Any]:
        return {
            'name': self.name,
            'model': None,
        }

    def __str__(self) -> str:
        return f'Model {self.name}'

    def __repr__(self) -> str:
        return self.__str__()
