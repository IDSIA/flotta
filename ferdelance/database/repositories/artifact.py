from ferdelance.config import config_manager
from ferdelance.core.artifacts import Artifact, ArtifactStatus
from ferdelance.database.tables import Artifact as ArtifactDB
from ferdelance.database.repositories.core import AsyncSession, Repository
from ferdelance.logging import get_logger
from ferdelance.schemas.database import ServerArtifact
from ferdelance.shared.status import ArtifactJobStatus

from sqlalchemy import func, select
from sqlalchemy.exc import NoResultFound

from pathlib import Path
from uuid import uuid4

import aiofiles
import aiofiles.os as aos
import json


LOGGER = get_logger(__name__)


def view(artifact: ArtifactDB) -> ServerArtifact:
    return ServerArtifact(
        id=artifact.id,
        creation_time=artifact.creation_time,
        path=Path(artifact.path),
        status=ArtifactJobStatus[artifact.status],
        iteration=artifact.iteration,
    )


class ArtifactRepository(Repository):
    """A repository used to store and manage all artifacts received by the server
    from the workbenches. The content of an artifact is written to disk, while a
    record is inserted in the database.
    """

    def __init__(self, session: AsyncSession) -> None:
        super().__init__(session)

    async def create_artifact(self, artifact: Artifact) -> ServerArtifact:
        """Creates a new artifact. The new artifact will be stored on disk and
        will have a SCHEDULED status. Exceptions are raised if we are trying
        to create an existing artifact and if the artifact is malformed.

        Args:
            artifact (Artifact):
                Artifact received by a workbench.

        Raises:
            ValueError:
                If the artifact already exists.
            ValueError:
                If the artifact is malformed.

        Returns:
            ServerArtifact:
                An handler for the server representation of the artifact.
        """

        if not artifact.id:
            artifact.id = str(uuid4())
        else:
            existing = await self.session.scalar(
                select(func.count()).select_from(ArtifactDB).where(ArtifactDB.id == artifact.id)
            )

            if existing:
                raise ValueError("artifact already exists!")

        status = ArtifactJobStatus.SCHEDULED

        path = await self.store(artifact)

        db_artifact = ArtifactDB(
            id=artifact.id,
            path=str(path),
            status=status.name,
        )

        self.session.add(db_artifact)
        await self.session.commit()
        await self.session.refresh(db_artifact)

        LOGGER.info(f"artifact={db_artifact.id}: created in status={db_artifact.status}")

        return view(db_artifact)

    async def storage_location(self, artifact_id: str, filename: str = "artifact.json") -> Path:
        """Checks that the output directory for this artifact exists. If not
        it will be created. Then it creates a path for the destination file.

        The artifact file is supposed to be stored in the JSON format.

        Args:
            artifact_id (str):
                Id of the artifact to save to or get from disk.
            filename (str, optional):
                Filename of the destination object.
                Defaults to "artifact.json".

        Returns:
            str:
                A valid path to the directory where the artifact can be saved
                to or loaded from. Path is considered to be a JSON file.
        """

        path = config_manager.get().storage_artifact(artifact_id)
        await aos.makedirs(path, exist_ok=True)
        return path / filename

    async def store(self, artifact: Artifact) -> Path:
        """Save an artifact on disk in JSON format.

        Args:
            artifact (Artifact):
                The artifact content that will be saved on disk.

        Returns:
            str:
                The path where the data have been saved to.
        """
        if not artifact.id:
            raise ValueError("Artifact not initialized")

        path = await self.storage_location(artifact.id)

        async with aiofiles.open(path, "w") as f:
            content = json.dumps(artifact.dict(), indent=True)
            await f.write(content)

        LOGGER.info(f"artifact={artifact.id}: stored descriptor to path={path}")

        return path

    async def load(self, artifact_id: str) -> Artifact:
        """Load an artifact from disk given its id, if it has been found on
        disk and in the database.

        Args:
            artifact_id (str):
                Id of the artifact to load.

        Raises:
            ValueError:
                If the artifact path has not been found on disk.
            ValueError:
                If the artifact_id was not found in the database.

        Returns:
            Artifact:
                The requested artifact.
        """

        try:
            artifact_path: Path = await self.storage_location(artifact_id)

            LOGGER.info(f"artifact={artifact_id}: request loading from path={artifact_path}")

            if not await aos.path.exists(artifact_path):
                raise ValueError(f"artifact={artifact_id} not found")

            async with aiofiles.open(artifact_path, "r") as f:
                content = await f.read()
                return Artifact(**json.loads(content))

        except NoResultFound:
            raise ValueError(f"artifact={artifact_id} not found")

    async def get_artifact(self, artifact_id: str) -> ServerArtifact:
        """Returns the requested artifact handler, not the real artifact. Use the
        load() method to load the artifact from disk.

        Args:
            artifact_id (str):
                Id of the artifact to get from the database.

        Raises:
            NoResultFound:
                If the artifact does not exists.

        Returns:
            ServerArtifact:
                A server handler for the requested artifact.
        """
        res = await self.session.scalars(select(ArtifactDB).where(ArtifactDB.id == artifact_id).limit(1))

        return view(res.one())

    async def get_artifact_path(self, artifact_id: str) -> Path:
        """Returns the path to the given artifact saved on disk, if the
        artifact exists.

        Args:
            artifact_id (str):
                Id of the artifact to get the path.

        Raises:
            NoResultFound:
                If the artifact_id does not exists or if on disk there are no
                files saved.

        Returns:
            str:
                The requested path.
        """
        res = await self.session.scalars(select(ArtifactDB.path).where(ArtifactDB.id == artifact_id).limit(1))

        path = Path(res.one())

        if aos.path.exists(path):
            return path

        raise NoResultFound()

    async def list_artifacts(self) -> list[ServerArtifact]:
        """List all artifacts registered in the database.

        Returns:
            list[ServerArtifact]:
                A list of server handlers to artifacts. Note that this list
                can be an empty list.
        """
        res = await self.session.execute(select(ArtifactDB))
        artifact_db_list = res.scalars().all()
        return [view(a) for a in artifact_db_list]

    async def get_status(self, artifact_id: str) -> ArtifactStatus:
        """Returns the current status for the given artifact_id, if it exists.

        Args:
            artifact_id (str):
                Id of the artifact to get the status for.

        Raises:
            NoResultFound:
                If the artifact_id does not exists.

        Returns:
            ArtifactStatus:
                The current status of the artifact.
        """
        res = await self.session.scalars(select(ArtifactDB).where(ArtifactDB.id == artifact_id).limit(1))

        artifact = res.one()

        return ArtifactStatus(
            id=artifact.id,
            status=ArtifactJobStatus[artifact.status],
            iteration=artifact.iteration,
        )

    async def update_status(
        self, artifact_id: str, new_status: ArtifactJobStatus | None = None, iteration: int = -1
    ) -> ServerArtifact:
        """Updated the current status of the given artifact with the new status
        provided, if the artifact exists.

        Args:
            artifact_id (str):
                Id of the artifact to update.
            new_status (ArtifactJobStatus):
                New status of the artifact.
            iteration (int):
                Index of the last updated iteration.

        Raises:
            NoResultFound:
                If the artifact_id does not exists in the database.
        """
        res = await self.session.scalars(select(ArtifactDB).where(ArtifactDB.id == artifact_id))

        artifact: ArtifactDB = res.one()

        LOGGER.info(
            f"artifact={artifact_id}: updating artifact status "
            f"from status={artifact.status} it={artifact.iteration} "
            f"to status={new_status} it={iteration}"
        )

        dirty = False

        if new_status is not None:
            if artifact.status != new_status.name:
                artifact.status = new_status.name
                dirty = True

        if iteration > -1:
            if artifact.iteration != iteration:
                artifact.iteration = iteration
                dirty = True

        if not dirty:
            LOGGER.info(f"artifact={artifact_id}: no status update required")
            return view(artifact)

        await self.session.commit()
        await self.session.refresh(artifact)

        return view(artifact)

    async def mark_error(self, artifact_id: str, iteration: int = -1) -> ServerArtifact:
        status = await self.update_status(artifact_id, ArtifactJobStatus.ERROR, iteration)
        return status
