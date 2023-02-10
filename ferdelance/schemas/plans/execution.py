from pydantic import BaseModel


class ExecutionPlan(BaseModel):
    """Describe how to split and evaluate data from a datasource in
    train/test/validation.
    """

    test_percentage: float = 0.0
    val_percentage: float = 0.0
    random_seed: float | None = None

    # TODO: implement this!
