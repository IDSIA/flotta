from typing import Any

from pydantic import BaseModel, model_validator

class_registry: dict[str, Any] = dict()


class Entity(BaseModel):
    entity: str = ""
    random_state: Any = None

    # register subclasses when created
    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        if cls.__name__ not in class_registry:
            class_registry[cls.__name__] = cls

    # set the _name field with the correct class-name
    @model_validator(mode="after")
    def set_entity_type(self):
        self.entity = type(self).__name__
        return self

    @model_validator(mode="before")
    def set_entities(cls, values: dict[str, Any]) -> Any:
        return create_entities(values)


def class_from_name(args: dict[str, Any]) -> Any:
    class_name = args["entity"]
    subclass = class_registry[class_name]
    return subclass(**args)


def create_entities(values: Any) -> Any:
    if not isinstance(values, dict):
        return values

    for key, args in values.items():

        if isinstance(args, list):

            for index, params in enumerate(args):
                if isinstance(params, dict) and "entity" in params:
                    values[key][index] = class_from_name(params)

                else:
                    create_entities(params)

        elif isinstance(args, dict):
            params = args

            if "entity" in params:
                values[key] = class_from_name(params)

    return values


class_registry[Entity.__name__] = Entity
