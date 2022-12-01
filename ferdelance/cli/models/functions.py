import pandas as pd

from ...database import get_session
from ...database.services import ModelService
from ...database.tables import Model


async def models_list(**kwargs) -> pd.DataFrame:

    artifact_id = kwargs.get("artifact_id", None)
    model_id = kwargs.get("client_id", None)

    session = await anext(get_session())

    ms = ModelService(session)

    print("===============================")
    print(ms)
    print("===============================")

    if artifact_id is not None:
        model_sessions: list[Model] = await ms.get_models_by_artifact_id(artifact_id)
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

    return pd.DataFrame(model_list)


async def models_create(**kwargs) -> None:
    pass
