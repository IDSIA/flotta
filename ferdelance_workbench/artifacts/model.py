from typing import Any


class Model:

    def __init__(self, name: str, model: str | None = None) -> None:
        self.name = name
        self.model = model

    def json(self) -> dict[str, Any]:
        return {
            'name': self.name,
            'model': None,
        }

    def __str__(self) -> str:
        return f'Model {self.name}'

    def __repr__(self) -> str:
        return self.__str__()


def model_from_json(data: dict[str, Any]) -> Model:
    return Model(data['name'], data['model'])
