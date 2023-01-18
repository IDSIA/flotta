"""Implementation of the CLI features regarding artifacts
"""

from typing import List

import pandas as pd

from ....database import DataBase
from ....database.schemas import Artifact
from ....database.services import ArtifactService
from ...visualization import show_many, show_one


async def list_artifacts() -> List[Artifact]:
    """Print and Return Artifact objects list

    Returns:
        List[Artifact]: List of Artifact objects
    """
    db = DataBase()
    async with db.async_session() as session:
        artifact_service: ArtifactService = ArtifactService(session)
        artifacts: List[Artifact] = await artifact_service.get_artifact_list()
        show_many(artifacts)
        return artifacts


async def describe_artifact(artifact_id: str) -> Artifact:
    """Print and return a single Artifact object

    Args:
        artifact_id (str, optional): Which artifact to describe.

    Raises:
        ValueError: if no artifact id is provided

    Returns:
        Artifact: The Artifact object
    """
    if artifact_id is None:
        raise ValueError("Provide an Artifact ID")

    db = DataBase()
    async with db.async_session() as session:
        artifact_service: ArtifactService = ArtifactService(session)
        artifact: Artifact | None = await artifact_service.get_artifact(artifact_id=artifact_id)
        if artifact is None:
            print(f"No artifact found with id {artifact_id}")
        else:
            show_one(artifact)
        return artifact
