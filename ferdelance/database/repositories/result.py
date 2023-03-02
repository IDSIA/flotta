from ferdelance.config import conf
from ferdelance.schemas.database import Result
from ferdelance.database.tables import Result as ResultDB
from ferdelance.database.repositories.core import AsyncSession, Repository

from sqlalchemy import select
from uuid import uuid4

import os


def view(result: ResultDB) -> Result:
    return Result(
        result_id=result.result_id,
        artifact_id=result.artifact_id,
        client_id=result.component_id,
        path=result.path,
        creation_time=result.creation_time,
        is_estimator=result.is_estimator,
        is_model=result.is_model,
        is_aggregated=result.is_aggregated,
    )


class ResultRepository(Repository):
    def __init__(self, session: AsyncSession) -> None:
        super().__init__(session)

    def storage_dir(self, artifact_id) -> str:
        out_dir = os.path.join(conf.STORAGE_ARTIFACTS, artifact_id)
        os.makedirs(out_dir, exist_ok=True)
        return out_dir

    async def create_result_estimator(self, artifact_id: str, client_id: str) -> Result:
        result_id: str = str(uuid4())

        out_path = os.path.join(
            self.storage_dir(artifact_id), f"{artifact_id}.{client_id}.{result_id}.PARTIAL.estimator"
        )

        result_db = ResultDB(
            result_id=result_id,
            path=out_path,
            artifact_id=artifact_id,
            component_id=client_id,
            is_estimator=True,
        )

        self.session.add(result_db)
        await self.session.commit()
        await self.session.refresh(result_db)

        return view(result_db)

    async def create_result_estimator_aggregated(self, artifact_id: str, client_id: str) -> Result:
        result_id: str = str(uuid4())

        filename = f"{artifact_id}.{result_id}.AGGREGATED.estimator"
        out_path = os.path.join(self.storage_dir(artifact_id), filename)

        result_db = ResultDB(
            result_id=result_id,
            path=out_path,
            artifact_id=artifact_id,
            component_id=client_id,
            is_estimator=True,
            is_aggregated=True,
        )

        self.session.add(result_db)
        await self.session.commit()
        await self.session.refresh(result_db)

        return view(result_db)

    async def create_result_model(self, artifact_id: str, client_id: str) -> Result:
        result_id: str = str(uuid4())

        out_path = os.path.join(self.storage_dir(artifact_id), f"{artifact_id}.{client_id}.{result_id}.PARTIAL.model")

        result_db = ResultDB(
            result_id=result_id,
            path=out_path,
            artifact_id=artifact_id,
            component_id=client_id,
            is_model=True,
        )

        self.session.add(result_db)
        await self.session.commit()
        await self.session.refresh(result_db)

        return view(result_db)

    async def create_result_model_aggregated(self, artifact_id: str, client_id: str) -> Result:
        result_id: str = str(uuid4())

        filename = f"{artifact_id}.{result_id}.AGGREGATED.model"
        out_path = os.path.join(self.storage_dir(artifact_id), filename)

        result_db = ResultDB(
            result_id=result_id,
            path=out_path,
            artifact_id=artifact_id,
            component_id=client_id,
            is_model=True,
            is_aggregated=True,
        )

        self.session.add(result_db)
        await self.session.commit()
        await self.session.refresh(result_db)

        return view(result_db)

    async def get_by_id(self, result_id: str) -> Result:
        """Can raise NoResultFound"""
        res = await self.session.scalars(select(ResultDB).where(ResultDB.result_id == result_id))
        return view(res.one())

    async def get_model_by_id(self, model_id: str) -> Result:
        """Can raise NoResultFound"""
        res = await self.session.scalars(
            select(ResultDB).where(
                ResultDB.result_id == model_id,
                ResultDB.is_model,
            )
        )
        return view(res.one())

    async def get_models_by_artifact_id(self, artifact_id: str) -> list[Result]:
        res = await self.session.scalars(
            select(ResultDB).where(
                ResultDB.artifact_id == artifact_id,
                ResultDB.is_model,
            )
        )
        result_list = [view(m) for m in res.all()]
        return result_list

    async def get_model_list(self) -> list[Result]:
        res = await self.session.scalars(
            select(ResultDB).where(
                ResultDB.is_model,
            )
        )
        result_list = [view(m) for m in res.all()]
        return result_list

    async def get_aggregated_model(self, artifact_id: str) -> Result:
        """Can raise NoResultFound"""
        res = await self.session.scalars(
            select(ResultDB).where(
                ResultDB.artifact_id == artifact_id,
                ResultDB.is_aggregated,
                ResultDB.is_model,
            )
        )
        return view(res.one())

    async def get_partial_model(self, artifact_id: str, client_id: str) -> Result:
        """Can raise NoResultFound"""
        res = await self.session.scalars(
            select(ResultDB).where(
                ResultDB.artifact_id == artifact_id,
                ResultDB.component_id == client_id,
                ResultDB.is_model,
            )
        )
        return view(res.one())
