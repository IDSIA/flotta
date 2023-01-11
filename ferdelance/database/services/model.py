from .core import DBSessionService, AsyncSession
from ..tables import Model
from ...config import conf

from sqlalchemy import select
from uuid import uuid4

import os


class ModelService(DBSessionService):

    def __init__(self, session: AsyncSession) -> None:
        super().__init__(session)

    def storage_dir(self, artifact_id) -> str:
        out_dir = os.path.join(conf.STORAGE_ARTIFACTS, artifact_id)
        os.makedirs(out_dir, exist_ok=True)
        return out_dir

    async def create_model_aggregated(self, artifact_id: str, client_id: str) -> Model:
        model_id: str = str(uuid4())

        filename = f'{artifact_id}.{model_id}.AGGREGATED.model'
        out_path = os.path.join(self.storage_dir(artifact_id), filename)

        model_db = Model(
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

    async def create_local_model(self, artifact_id: str, client_id) -> Model:
        model_id: str = str(uuid4())

        filename = f'{artifact_id}.{client_id}.{model_id}.model'
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

    async def get_model_by_id(self, model_id: str) -> Model | None:
        res = await self.session.execute(select(Model).where(Model.model_id == model_id).limit(1))
        return res.scalar_one_or_none()

    async def get_models_by_artifact_id(self, artifact_id: str) -> list[Model]:
        res = await self.session.execute(select(Model).where(Model.artifact_id == artifact_id))
        return res.scalars().all()

    async def get_model_list(self) -> list[Model]:
        res = await self.session.execute(select(Model))
        return res.scalars().all()
