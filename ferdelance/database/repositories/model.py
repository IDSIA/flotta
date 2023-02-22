from ferdelance.config import conf
from ferdelance.schemas.database import ServerModel
from ferdelance.database.tables import Model as ModelDB
from ferdelance.database.repositories.core import AsyncSession, Repository

from sqlalchemy import select
from uuid import uuid4

import os


def view(model: ModelDB) -> ServerModel:
    return ServerModel(
        model_id=model.model_id,
        creation_time=model.creation_time,
        path=model.path,
        aggregated=model.aggregated,
        artifact_id=model.artifact_id,
        client_id=model.component_id,
    )


class ModelRepository(Repository):
    def __init__(self, session: AsyncSession) -> None:
        super().__init__(session)

    def storage_dir(self, artifact_id) -> str:
        out_dir = os.path.join(conf.STORAGE_ARTIFACTS, artifact_id)
        os.makedirs(out_dir, exist_ok=True)
        return out_dir

    async def create_model_aggregated(self, artifact_id: str, client_id: str) -> ServerModel:
        model_id: str = str(uuid4())

        filename = f"{artifact_id}.{model_id}.AGGREGATED.model"
        out_path = os.path.join(self.storage_dir(artifact_id), filename)

        model_db = ModelDB(
            model_id=model_id,
            path=out_path,
            artifact_id=artifact_id,
            component_id=client_id,
            aggregated=True,
        )

        self.session.add(model_db)
        await self.session.commit()
        await self.session.refresh(model_db)

        return view(model_db)

    async def create_local_model(self, artifact_id: str, client_id) -> ServerModel:
        model_id: str = str(uuid4())

        filename = f"{artifact_id}.{client_id}.{model_id}.model"
        out_path = os.path.join(self.storage_dir(artifact_id), filename)

        model_db = ModelDB(
            model_id=model_id,
            path=out_path,
            artifact_id=artifact_id,
            component_id=client_id,
            aggregated=False,
        )

        self.session.add(model_db)
        await self.session.commit()
        await self.session.refresh(model_db)

        return view(model_db)

    async def get_model_by_id(self, model_id: str) -> ServerModel:
        """Can raise NoResultFound"""
        query = await self.session.execute(select(ModelDB).where(ModelDB.model_id == model_id).limit(1))
        res: ModelDB = query.scalar_one()
        return view(res)

    async def get_models_by_artifact_id(self, artifact_id: str) -> list[ServerModel]:
        query = await self.session.execute(select(ModelDB).where(ModelDB.artifact_id == artifact_id))
        res = query.scalars().all()
        model_list = [view(m) for m in res]
        return model_list

    async def get_model_list(self) -> list[ServerModel]:
        query = await self.session.execute(select(ModelDB))
        res = query.scalars().all()
        model_list = [view(m) for m in res]
        return model_list

    async def get_aggregated_model(self, artifact_id: str) -> ServerModel:
        """Can raise NoResultFound"""
        res = await self.session.execute(select(ModelDB).where(ModelDB.artifact_id == artifact_id, ModelDB.aggregated))
        return view(res.scalar_one())

    async def get_partial_model(self, artifact_id: str, client_id: str) -> ServerModel:
        """Can raise NoResultFound"""
        res = await self.session.execute(
            select(ModelDB).where(ModelDB.artifact_id == artifact_id, ModelDB.component_id == client_id)
        )
        return view(res.scalar_one())
