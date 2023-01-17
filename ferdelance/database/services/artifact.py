from sqlalchemy import func, select

from ferdelance.shared.status import ArtifactJobStatus

from ..schemas import Artifact as ArtifactView
from ..schemas import Model as ModelView
from ..tables import Artifact as ArtifactDB
from ..tables import Model as ModelDB
from .core import AsyncSession, DBSessionService


def get_view(artifact: ArtifactDB) -> ArtifactView:
    return ArtifactView(**artifact.__dict__)


class ArtifactService(DBSessionService):
    def __init__(self, session: AsyncSession) -> None:
        super().__init__(session)

    async def get_artifact_list(self) -> list[ArtifactView]:
        res = await self.session.execute(select(ArtifactDB))
        artifact_db_list = res.scalars().all()
        return [get_view(a) for a in artifact_db_list]

    async def create_artifact(
        self, artifact_id: str, path: str, status: str
    ) -> ArtifactView:
        db_artifact = ArtifactDB(artifact_id=artifact_id, path=path, status=status)

        existing = await self.session.scalar(
            select(func.count())
            .select_from(ArtifactDB)
            .where(ArtifactDB.artifact_id == artifact_id)
        )

        if existing > 0:
            raise ValueError("artifact already exists!")

        self.session.add(db_artifact)
        await self.session.commit()
        await self.session.refresh(db_artifact)

        return get_view(db_artifact)

    async def get_artifact(self, artifact_id: str) -> ArtifactDB | None:
        query = await self.session.execute(
            select(ArtifactDB).where(ArtifactDB.artifact_id == artifact_id).limit(1)
        )
        res = query.scalar_one_or_none()
        if res:
            return get_view(res)
        return res

    async def get_aggregated_model(self, artifact_id: str) -> ModelView:
        res = await self.session.execute(
            select(ModelDB).where(
                ModelDB.artifact_id == artifact_id, ModelDB.aggregated
            )
        )
        return get_view(res.scalar_one())

    async def get_partial_model(self, artifact_id: str, client_id: str) -> ModelView:
        res = await self.session.execute(
            select(ModelDB).where(
                ModelDB.artifact_id == artifact_id, ModelDB.client_id == client_id
            )
        )
        return get_view(res.scalar_one())

    async def update_status(
        self, artifact_id: str, new_status: ArtifactJobStatus
    ) -> None:
        artifact: ArtifactDB = await self.session.scalar(
            select(ArtifactDB).where(ArtifactDB.artifact_id == artifact_id)
        )
        artifact.status = new_status.name

        await self.session.commit()
