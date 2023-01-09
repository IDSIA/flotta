from ferdelance.shared.status import ArtifactJobStatus
from sqlalchemy import func, select

from ..tables import Artifact, Model
from .core import AsyncSession, DBSessionService


class ArtifactService(DBSessionService):
    def __init__(self, session: AsyncSession) -> None:
        super().__init__(session)

    async def get_artifact_list(self) -> list[Artifact]:
        res = await self.session.execute(select(Artifact))
        return res.scalars().all()

    async def create_artifact(
        self, artifact_id: str, path: str, status: str
    ) -> Artifact:
        db_artifact = Artifact(artifact_id=artifact_id, path=path, status=status)

        existing = await self.session.scalar(
            select(func.count())
            .select_from(Artifact)
            .where(Artifact.artifact_id == artifact_id)
        )

        if existing > 0:
            raise ValueError("artifact already exists!")

        self.session.add(db_artifact)
        await self.session.commit()
        await self.session.refresh(db_artifact)

        return db_artifact

    async def get_artifact(self, artifact_id: str) -> Artifact | None:
        res = await self.session.execute(
            select(Artifact).where(Artifact.artifact_id == artifact_id).limit(1)
        )
        return res.scalar_one_or_none()

    async def get_aggregated_model(self, artifact_id: str) -> Model:
        res = await self.session.execute(
            select(Model).where(Model.artifact_id == artifact_id, Model.aggregated)
        )
        return res.scalar_one()

    async def get_partial_model(self, artifact_id: str, client_id: str) -> Model:
        res = await self.session.execute(
            select(Model).where(
                Model.artifact_id == artifact_id, Model.client_id == client_id
            )
        )
        return res.scalar_one()

    async def update_status(
        self, artifact_id: str, new_status: ArtifactJobStatus
    ) -> None:
        artifact: Artifact = await self.session.scalar(
            select(Artifact).where(Artifact.artifact_id == artifact_id)
        )
        artifact.status = new_status.name

        await self.session.commit()
