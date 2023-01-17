"""Implementation of the CLI features regarding models
"""

import pandas as pd

from ...database import DataBase
from ...database.schemas import Model
from ...database.services import ModelService


async def models_list(artifact_id: str = None) -> pd.DataFrame:
    """Print model list, with or without filters on ARTIFACT_ID, MODEL_ID"""
    # TODO depending on 1:1, 1:n relations with artifacts arguments change or disappear

    db = DataBase()
    async with db.async_session() as session:

        ms = ModelService(session)

        if artifact_id is not None:
            model_sessions: list[Model] = await ms.get_models_by_artifact_id(
                artifact_id
            )
        else:
            model_sessions: list[Model] = await ms.get_model_list()

        model_list = [m.dict() for m in model_sessions]

        result: pd.DataFrame = pd.DataFrame(model_list)

        print(result)

        return result


async def describe_model(model_id: str = None) -> Model:
    """Describe single model by printing its db record.

    Args:
        model_id (str): Unique ID of the model

    Raises:
        ValueError: if no model id is provided

    Returns:
        Model: the Model object
    """

    if model_id is None:
        raise ValueError("Provide a Model ID")

    db = DataBase()
    async with db.async_session() as session:

        ms = ModelService(session)

        model: Model | None = await ms.get_model_by_id(model_id)

        if model is None:
            print(f"No model found with id {model_id}")
        else:
            print(pd.Series(model.dict()))

        return model
