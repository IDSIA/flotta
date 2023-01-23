"""Implementation of the CLI features regarding artifacts"""

import pandas as pd

from ferdelance.database import DataBase
from ferdelance.database.schemas import Artifact
from ferdelance.database.services import ArtifactService
from ferdelance.cli.visualization import show_many, show_one

from sqlalchemy.exc import NoResultFound


async def list_artifacts() -> list[Artifact]:
    """Print and Return Artifact objects list

    Returns:
        List[Artifact]: List of Artifact objects
    """
    db = DataBase()
    async with db.async_session() as session:
        artifact_service: ArtifactService = ArtifactService(session)
        artifacts: list[Artifact] = await artifact_service.get_artifact_list()
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

        try:
            artifact: Artifact = await artifact_service.get_artifact(artifact_id=artifact_id)
            show_one(artifact)
            return artifact

        except NoResultFound as e:
            print(f"No artifact found with id {artifact_id}")
            raise e
