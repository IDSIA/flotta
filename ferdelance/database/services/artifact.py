from ferdelance.database.tables import Artifact as ArtifactDB
from ferdelance.database.services.core import AsyncSession, DBSessionService
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
    )


class ArtifactService(DBSessionService):
    def __init__(self, session: AsyncSession) -> None:
        super().__init__(session)

    async def create_artifact(self, artifact: Artifact) -> ServerArtifact:
        """Can raise ValueError."""

        if artifact.artifact_id is None:
            artifact.artifact_id = str(uuid4())
        else:
            existing = await self.session.scalar(
                select(func.count()).select_from(ArtifactDB).where(ArtifactDB.artifact_id == artifact.artifact_id)
            )

            if existing:
                raise ValueError("artifact already exists!")

        status = ArtifactJobStatus.SCHEDULED.name

        path = await self.store(artifact)

        db_artifact = ArtifactDB(
            artifact_id=artifact.artifact_id,
            path=path,
            status=status,
        )

        self.session.add(db_artifact)
        await self.session.commit()
        await self.session.refresh(db_artifact)

        return view(db_artifact)

    async def storage_location(self, artifact_id: str, filename: str = "artifact.json") -> str:
        path = conf.storage_dir_artifact(artifact_id)
        await aos.makedirs(path, exist_ok=True)
        return os.path.join(path, filename)

    async def store(self, artifact: Artifact) -> str:
        if artifact.artifact_id is None:
            raise ValueError("Artifact not initialized")
        path = await self.storage_location(artifact.artifact_id)

        async with aiofiles.open(path, "w") as f:
            content = json.dumps(artifact.dict())
            await f.write(content)

        return path

    async def load(self, artifact_id: str) -> Artifact:
        """Can raise ValueError."""
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
        """Can raise NoResultFound."""
        res = await self.session.scalars(select(ArtifactDB).where(ArtifactDB.artifact_id == artifact_id).limit(1))

        return view(res.one())

    async def get_artifact_path(self, artifact_id: str) -> str:
        """Can raise NoResultFound."""
        res = await self.session.scalars(select(ArtifactDB.path).where(ArtifactDB.artifact_id == artifact_id).limit(1))

        path = res.one()

        if aos.path.exists(path):
            return path

        raise NoResultFound()

    async def list(self) -> list[ServerArtifact]:
        res = await self.session.execute(select(ArtifactDB))
        artifact_db_list = res.scalars().all()
        return [view(a) for a in artifact_db_list]

    async def get_status(self, artifact_id: str) -> ArtifactStatus:
        """Can raise NoResultFound."""
        res = await self.session.scalars(select(ArtifactDB).where(ArtifactDB.artifact_id == artifact_id).limit(1))

        artifact = res.one()

        return ArtifactStatus(
            artifact_id=artifact.artifact_id,
            status=artifact.status,
        )

    async def update_status(self, artifact_id: str, new_status: ArtifactJobStatus) -> None:
        """Can raise NoResultFound."""
        res = await self.session.scalars(select(ArtifactDB).where(ArtifactDB.artifact_id == artifact_id))

        artifact: ArtifactDB = res.one()
        artifact.status = new_status.name

        await self.session.commit()
