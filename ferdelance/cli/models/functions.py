"""Implementation of the CLI features regarding models
"""

from pathlib import Path
from uuid import uuid4

import pandas as pd

from ...config import STORAGE_ARTIFACTS
from ...database import DataBase
from ...database.services import ModelService
from ...database.tables import Model


async def models_list(**kwargs) -> pd.DataFrame:
    """Print model list, with or without filters on ARTIFACT_ID, MODEL_ID"""
    artifact_id = kwargs.get("artifact_id", None)
    model_id = kwargs.get("client_id", None)

    db = DataBase()
    async with db.async_session() as session:

        ms = ModelService(session)

        if artifact_id is not None:
            model_sessions: list[Model] = await ms.get_models_by_artifact_id(
                artifact_id
            )
        elif model_id is not None:
            model_sessions: list[Model] = await ms.get_model_by_id(model_id)
        else:
            model_sessions: list[Model] = await ms.get_model_list()

        model_list = [
            {
                "model_id": m.model_id,
                "artifact_id": m.artifact_id,
                "client_id": m.client_id,
                "aggregated": m.aggregated,
                "creation_time": m.creation_time,
            }
            for m in model_sessions
        ]

        result: pd.DataFrame = pd.DataFrame(model_list)

        print(result)

        return result
