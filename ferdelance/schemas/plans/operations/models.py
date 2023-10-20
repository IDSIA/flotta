from ferdelance.schemas.plans.operations.core import Operation

from pydantic import validator


class Define(Operation):
    # TODO: check how fdl.schemas.models.core work and adapt them to work there

    @validator("data_names")
    def set_data_names(cls, _) -> list[str]:
        return list()
