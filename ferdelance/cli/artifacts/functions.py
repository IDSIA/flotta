"""Implementation of the CLI features regarding artifacts
"""

import pandas as pd

from ...database import DataBase
from ...database.schemas import Artifact
from ...database.services import ArtifactService


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


async def get_artifact_description(artifact_id: str = None) -> Artifact:
    """Print artifacts list"""

    if artifact_id is None:
        raise ValueError("artifact_id is None, must have a value")

    db = DataBase()
    async with db.async_session() as session:

        artifact_service: ArtifactService = ArtifactService(session)

        artifact: Artifact | None = await artifact_service.get_artifact(
            artifact_id=artifact_id
        )

        if artifact is None:
            print(f"No artifact found with id {artifact_id}")
        else:
            print(pd.Series(artifact.dict()))

        return artifact
