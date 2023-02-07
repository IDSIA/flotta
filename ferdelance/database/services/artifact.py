from ferdelance.database.tables import Artifact as ArtifactDB
from ferdelance.database.services.core import AsyncSession, DBSessionService
from ferdelance.schemas.database import ServerArtifact
from ferdelance.schemas.artifacts import ArtifactStatus
from ferdelance.shared.status import ArtifactJobStatus

from sqlalchemy import func, select


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

    async def get_artifact_list(self) -> list[ServerArtifact]:
        res = await self.session.execute(select(ArtifactDB))
        artifact_db_list = res.scalars().all()
        return [view(a) for a in artifact_db_list]

    async def create_artifact(self, artifact_id: str, path: str, status: str) -> ServerArtifact:
        """Can raise ValueError"""
        db_artifact = ArtifactDB(artifact_id=artifact_id, path=path, status=status)

        existing = await self.session.scalar(
            select(func.count()).select_from(ArtifactDB).where(ArtifactDB.artifact_id == artifact_id)
        )

        if existing:
            raise ValueError("artifact already exists!")

        self.session.add(db_artifact)
        await self.session.commit()
        await self.session.refresh(db_artifact)

        return view(db_artifact)

    async def get_artifact(self, artifact_id: str) -> ServerArtifact:
        """Can raise NoResultException."""
        res = await self.session.scalars(select(ArtifactDB).where(ArtifactDB.artifact_id == artifact_id).limit(1))

        return view(res.one())

    async def get_artifact_path(self, artifact_id: str) -> str:
        """Can raise NoResultException."""
        res = await self.session.scalars(select(ArtifactDB.path).where(ArtifactDB.artifact_id == artifact_id).limit(1))

        return res.one()

    async def get_status(self, artifact_id: str) -> ArtifactStatus:
        """Can raise NoResultException."""
        res = await self.session.scalars(select(ArtifactDB).where(ArtifactDB.artifact_id == artifact_id).limit(1))

        artifact = res.one()

        return ArtifactStatus(
            artifact_id=artifact.artifact_id,
            status=artifact.status,
        )

    async def update_status(self, artifact_id: str, new_status: ArtifactJobStatus) -> None:
        """Can raise NoResultException."""
        res = await self.session.scalars(select(ArtifactDB).where(ArtifactDB.artifact_id == artifact_id))

        artifact: ArtifactDB = res.one()
        artifact.status = new_status.name

        await self.session.commit()
