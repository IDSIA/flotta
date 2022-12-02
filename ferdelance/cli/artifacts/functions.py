"""Implementation of the CLI features regarding artifacts
"""

import pandas as pd

from ...database import DataBase
from ...database.services import ArtifactService
from ...database.tables import Artifact


async def get_artifacts_list(**kwargs) -> pd.DataFrame:
    """Print artifacts list"""
    db = DataBase()
    async with db.async_session() as session:

        artifact_service: ArtifactService = ArtifactService(session)

        artifacts: list[Artifact] = await artifact_service.get_artifact_list()

        artifact_list = [a.dict() for a in artifacts]

        result: pd.DataFrame = pd.DataFrame(artifact_list)

        print(result)

        return result


async def get_artifact_description(**kwargs) -> pd.DataFrame:
    """Print artifacts list"""

    artifact_id = kwargs.get("artifact_id", None)

    if artifact_id is None:
        raise ValueError(f"artifact_id is None, must have a value")

    db = DataBase()
    async with db.async_session() as session:

        artifact_service: ArtifactService = ArtifactService(session)

        artifact: Artifact | None = await artifact_service.get_artifact(
            artifact_id=artifact_id
        )

        if artifact is None:
            print(f"No artifact found with id {artifact_id}")
        else:
            print(artifact.dict())

        return artifact
