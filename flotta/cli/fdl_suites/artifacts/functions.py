"""Implementation of the CLI features regarding artifacts"""

from ferdelance.cli.visualization import show_many, show_one
from ferdelance.database import DataBase
from ferdelance.database.repositories import ArtifactRepository
from ferdelance.schemas.database import ServerArtifact

from sqlalchemy.exc import NoResultFound


async def list_artifacts() -> list[ServerArtifact]:
    """Print and Return Artifact objects list

    Returns:
        List[Artifact]: List of Artifact objects
    """
    db = DataBase()
    async with db.async_session() as session:
        artifact_repository: ArtifactRepository = ArtifactRepository(session)
        artifacts: list[ServerArtifact] = await artifact_repository.list_artifacts()
        show_many(artifacts)
        return artifacts


async def describe_artifact(artifact_id: str) -> ServerArtifact | None:
    """Print and return a single Artifact object

    Args:
        artifact_id (str, optional): Which artifact to describe.

    Raises:
        ValueError: if no artifact id is provided

    Returns:
        Artifact: The Artifact object
    """
    if not artifact_id:
        raise ValueError("Provide an Artifact ID")

    db = DataBase()
    async with db.async_session() as session:
        artifact_repository: ArtifactRepository = ArtifactRepository(session)

        try:
            artifact: ServerArtifact = await artifact_repository.get_artifact(artifact_id)
            show_one(artifact)
            return artifact

        except NoResultFound:
            print(f"No artifact found with id {artifact_id}")
