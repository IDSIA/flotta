from ferdelance.database.tables import Artifact as ArtifactDB
from ferdelance.database.repositories.core import AsyncSession, Repository
from ferdelance.schemas.database import ServerArtifact
from ferdelance.schemas.artifacts import Artifact, ArtifactStatus
from ferdelance.shared.status import ArtifactJobStatus
from ferdelance.config import conf

from sqlalchemy import func, select
from sqlalchemy.exc import NoResultFound
from uuid import uuid4

import aiofiles
import aiofiles.os as aos
import json
import os


def view(artifact: ArtifactDB) -> ServerArtifact:
    return ServerArtifact(
        artifact_id=artifact.artifact_id,
        creation_time=artifact.creation_time,
        path=artifact.path,
        status=artifact.status,
        is_model=artifact.is_model,
        is_estimation=artifact.is_estimation,
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

        Note that an artifact can be a model or an estimation, not both and
        not none.

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

        if artifact.artifact_id is None:
            artifact.artifact_id = str(uuid4())
        else:
            existing = await self.session.scalar(
                select(func.count()).select_from(ArtifactDB).where(ArtifactDB.artifact_id == artifact.artifact_id)
            )

            if existing:
                raise ValueError("artifact already exists!")

        if artifact.is_model() and artifact.is_estimation():
            raise ValueError(f"invalid artifact_id={artifact.artifact_id} with both model and estimation")

        status = ArtifactJobStatus.SCHEDULED.name

        path = await self.store(artifact)

        db_artifact = ArtifactDB(
            artifact_id=artifact.artifact_id,
            path=path,
            status=status,
            is_model=artifact.is_model(),
            is_estimation=artifact.is_estimation(),
        )

        self.session.add(db_artifact)
        await self.session.commit()
        await self.session.refresh(db_artifact)

        return view(db_artifact)

    async def storage_location(self, artifact_id: str, filename: str = "artifact.json") -> str:
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

        path = conf.storage_dir_artifact(artifact_id)
        await aos.makedirs(path, exist_ok=True)
        return os.path.join(path, filename)

    async def store(self, artifact: Artifact) -> str:
        """Save an artifact on disk in JSON format.

        Args:
            artifact (Artifact):
                The artifact content that will be saved on disk.

        Returns:
            str:
                The path where the data have been saved to.
        """
        if artifact.artifact_id is None:
            raise ValueError("Artifact not initialized")
        path = await self.storage_location(artifact.artifact_id)

        async with aiofiles.open(path, "w") as f:
            content = json.dumps(artifact.dict())
            await f.write(content)

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
            artifact_path: str = await self.storage_location(artifact_id)

            if not await aos.path.exists(artifact_path):
                raise ValueError(f"artifact_id={artifact_id} not found")

            async with aiofiles.open(artifact_path, "r") as f:
                content = await f.read()
                return Artifact(**json.loads(content))

        except NoResultFound as _:
            raise ValueError(f"artifact_id={artifact_id} not found")

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
        res = await self.session.scalars(select(ArtifactDB).where(ArtifactDB.artifact_id == artifact_id).limit(1))

        return view(res.one())

    async def get_artifact_path(self, artifact_id: str) -> str:
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
        res = await self.session.scalars(select(ArtifactDB.path).where(ArtifactDB.artifact_id == artifact_id).limit(1))

        path = res.one()

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
        res = await self.session.scalars(select(ArtifactDB).where(ArtifactDB.artifact_id == artifact_id).limit(1))

        artifact = res.one()

        return ArtifactStatus(
            artifact_id=artifact.artifact_id,
            status=artifact.status,
        )

    async def update_status(self, artifact_id: str, new_status: ArtifactJobStatus) -> None:
        """Updated the current status of the given artifact with the new status
        provided, if the artifact exists.

        Args:
            artifact_id (str):
                Id of the artifact to update.
            new_status (ArtifactJobStatus):
                New status of the artifact.

        Raises:
            NoResultFound:
                If the artifact_id does not exists in the database.
        """
        res = await self.session.scalars(select(ArtifactDB).where(ArtifactDB.artifact_id == artifact_id))

        artifact: ArtifactDB = res.one()
        artifact.status = new_status.name

        await self.session.commit()
