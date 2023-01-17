import os
from uuid import uuid4

from sqlalchemy import select

from ...config import conf
from ..schemas import Model as ModelView
from ..tables import Model as ModelDB
from .core import AsyncSession, DBSessionService


def get_view(model: ModelDB) -> ModelView:
    return ModelView(**model.__dict__)


class ModelService(DBSessionService):
    def __init__(self, session: AsyncSession) -> None:
        super().__init__(session)

    def storage_dir(self, artifact_id) -> str:
        out_dir = os.path.join(conf.STORAGE_ARTIFACTS, artifact_id)
        os.makedirs(out_dir, exist_ok=True)
        return out_dir

    async def create_model_aggregated(
        self, artifact_id: str, client_id: str
    ) -> ModelView:
        model_id: str = str(uuid4())

        filename = f"{artifact_id}.{model_id}.AGGREGATED.model"
        out_path = os.path.join(self.storage_dir(artifact_id), filename)

        model_db = ModelDB(
            model_id=model_id,
            path=out_path,
            artifact_id=artifact_id,
            client_id=client_id,
            aggregated=True,
        )

        self.session.add(model_db)
        await self.session.commit()
        await self.session.refresh(model_db)

        return model_db

    async def create_local_model(self, artifact_id: str, client_id) -> ModelView:
        model_id: str = str(uuid4())

        filename = f"{artifact_id}.{client_id}.{model_id}.model"
        out_path = os.path.join(self.storage_dir(artifact_id), filename)

        model_db = Model(
            model_id=model_id,
            path=out_path,
            artifact_id=artifact_id,
            client_id=client_id,
            aggregated=False,
        )

        self.session.add(model_db)
        await self.session.commit()
        await self.session.refresh(model_db)

        return model_db

    async def get_model_by_id(self, model_id: str) -> ModelView | None:
        query = await self.session.execute(
            select(ModelDB).where(ModelDB.model_id == model_id).limit(1)
        )
        res: ModelDB | None = query.scalar_one_or_none()

        if res:
            return get_view(res)
        return res

    async def get_models_by_artifact_id(self, artifact_id: str) -> list[ModelView]:
        query = await self.session.execute(
            select(ModelDB).where(ModelDB.artifact_id == artifact_id)
        )
        res = query.scalars().all()
        model_list = [get_view(m) for m in res]
        return model_list

    async def get_model_list(self) -> list[ModelView]:
        query = await self.session.execute(select(ModelDB))
        res = query.scalars().all()
        model_list = [get_view(m) for m in res]
        return model_list
