from typing import Any

from pydantic import BaseModel, root_validator

class_registry: dict[str, Any] = dict()


class Entity(BaseModel):
    _name: str = ""
    random_state: Any = None

    # register subclasses when created
    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        if cls.__name__ not in class_registry:
            class_registry[cls.__name__] = cls

    # set the _name field with the correct class-name
    @root_validator(pre=False)
    def set_entity_type(cls, values):
        values["_name"] = cls.__name__
        return values

    @root_validator(pre=True)
    def set_entities(cls, values: Any) -> Any:
        return create_entities(values)


def class_from_name(entity: Any) -> Any:
    class_name = entity["_name"]
    subclass = class_registry[class_name]
    return subclass(**entity)


def create_entities(values: Any) -> Any:
    if not isinstance(values, dict):
        return values

    for key in values.keys():
        entities = values[key]

        if isinstance(entities, list):
            for index, entity in enumerate(entities):
                if isinstance(entity, dict) and "_name" in entity:
                    entities[index] = class_from_name(entity)
                else:
                    create_entities(entity)
        elif isinstance(entities, dict):
            entity = entities
            if "_name" in entity:
                values[key] = class_from_name(entity)

    return values


class_registry[Entity.__name__] = Entity
