from flotta.core.operations.core import Operation

from pydantic import field_validator


class Define(Operation):
    # TODO: check how fdl.schemas.models.core work and adapt them to work there

    @field_validator("data_names")
    def set_data_names(cls, _) -> list[str]:
        return list()
