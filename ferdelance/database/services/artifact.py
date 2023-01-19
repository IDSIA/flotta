from ferdelance.database.schemas import Artifact as ArtifactView
from ferdelance.database.tables import Artifact as ArtifactDB
from ferdelance.database.services.core import AsyncSession, DBSessionService

from ferdelance.shared.status import ArtifactJobStatus

from sqlalchemy import func, select


def view(artifact: ArtifactDB) -> ArtifactView:
    return ArtifactView(**artifact.__dict__)


class ArtifactService(DBSessionService):
    def __init__(self, session: AsyncSession) -> None:
        super().__init__(session)

    async def get_artifact_list(self) -> list[ArtifactView]:
        res = await self.session.execute(select(ArtifactDB))
        artifact_db_list = res.scalars().all()
        return [view(a) for a in artifact_db_list]

    async def create_artifact(self, artifact_id: str, path: str, status: str) -> ArtifactView:
        db_artifact = ArtifactDB(artifact_id=artifact_id, path=path, status=status)

        existing = await self.session.scalar(
            select(func.count()).select_from(ArtifactDB).where(ArtifactDB.artifact_id == artifact_id)
        )

        if existing > 0:
            raise ValueError("artifact already exists!")

        self.session.add(db_artifact)
        await self.session.commit()
        await self.session.refresh(db_artifact)

        return view(db_artifact)

    async def get_artifact(self, artifact_id: str) -> ArtifactView | None:
        query = await self.session.execute(select(ArtifactDB).where(ArtifactDB.artifact_id == artifact_id).limit(1))
        res = query.scalar_one_or_none()
        if res:
            return view(res)
        return res

    async def update_status(self, artifact_id: str, new_status: ArtifactJobStatus) -> None:
        artifact: ArtifactDB = await self.session.scalar(
            select(ArtifactDB).where(ArtifactDB.artifact_id == artifact_id)
        )
        artifact.status = new_status.name

        await self.session.commit()
