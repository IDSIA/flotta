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
        is_model=result.is_model,
        is_estimation=result.is_estimation,
        is_aggregation=result.is_aggregation,
    )


class ResultRepository(Repository):
    """A repository for the result of training and estimation tasks. This object
    can collect anything produced by clients (models, estimators) and workers
    (aggregated models).
    """

    def __init__(self, session: AsyncSession) -> None:
        super().__init__(session)

    def storage_directory(self, artifact_id: str) -> str:
        """Checks that the output directory for this result exists. If not it
        will be created.

        Args:
            artifact_id (str):
                Id of the artifact that the result belongs to.

        Returns:
            str:
                A valid path to the directory where a result can be saved to or loaded from.
        """
        out_dir = os.path.join(conf.STORAGE_ARTIFACTS, artifact_id)
        os.makedirs(out_dir, exist_ok=True)
        return out_dir

    async def create_result(
        self,
        artifact_id: str,
        producer_id: str,
        is_estimation: bool = False,
        is_model: bool = False,
        is_aggregation: bool = False,
        is_error: bool = False,
    ) -> Result:
        """Creates an entry in the database for the result produced by a client or a worker,
        identified with producer_id, and by setting the type of result as a specified by the flags.

        Args:
            artifact_id (str):
                The result will be produced and associated to this artifact_id
            producer_id (str):
                The component_id of whom has produced the result.
            is_estimation (bool, optional):
                Set to true when the result is an estimation.
                Defaults to False.
            is_model (bool, optional):
                Set to true when the result is a model.
                Defaults to False.
            is_aggregation (bool, optional):
                Set to true when the result is an aggregation.
                Defaults to False.
            is_error (bool, optional):
                Set to true when the result is an error.
                Defaults to False.

        Returns:
            Result:
                An handler to the recorded result in the database. This handler can be
                used to obtain the output path and save the result to disk.
        """

        result_id: str = str(uuid4())

        # name creation
        filename = f"{artifact_id}.{producer_id}.{result_id}"

        if is_error:
            filename += ".ERROR"
        elif is_aggregation:
            filename += ".AGGREGATED"
        else:
            filename += ".PARTIAL"

        if is_model:
            filename += ".model"
        elif is_estimation:
            filename += ".estimator"

        out_path = os.path.join(self.storage_directory(artifact_id), filename)

        result_db = ResultDB(
            result_id=result_id,
            path=out_path,
            artifact_id=artifact_id,
            component_id=producer_id,
            is_estimation=is_estimation,
            is_model=is_model,
            is_aggregation=is_aggregation,
            is_error=is_error,
        )

        self.session.add(result_db)
        await self.session.commit()
        await self.session.refresh(result_db)

        return view(result_db)

    async def get_by_id(self, result_id: str) -> Result:
        """Get the result given its result_id.

        Args:
            result_id (str):
                Id of the result to get.

        Raises:
            NoResultFound:
                If the result does not exists

        Returns:
            Result:
                The handler to the result, if one is found.
        """
        res = await self.session.scalars(
            select(ResultDB).where(
                ResultDB.result_id == result_id,
            )
        )

        return view(res.one())

    async def get_model_by_id(self, result_id: str) -> Result:
        """Get the result, considered a model, given its result_id.

        Args:
            result_id (str):
                Id of the result to get.

        Raises:
            NoResultFound:
                If the result does not exists

        Returns:
            Result:
                The handler to the result, if one is found.
        """
        res = await self.session.scalars(
            select(ResultDB).where(
                ResultDB.result_id == result_id,
                ResultDB.is_model,
            )
        )
        return view(res.one())

    async def get_estimator_by_id(self, result_id: str) -> Result:
        """Get the result, considered an estimation, given its result_id.

        Args:
            result_id (str):
                Id of the result to get.

        Raises:
            NoResultFound:
                If the result does not exists

        Returns:
            Result:
                The handler to the result, if one is found.
        """
        res = await self.session.scalars(
            select(ResultDB).where(
                ResultDB.result_id == result_id,
                ResultDB.is_estimation,
            )
        )
        return view(res.one())

    async def list_results_by_artifact_id(self, artifact_id: str) -> list[Result]:
        """Get a list of results associated with the given artifact_id. This
        returns all kind of results, models and estimations, aggregated or not.

        Args:
            artifact_id (str):
                Id of the artifact to search for.

        Returns:
            Result:
                A list of all the results associated with the given artifact_id.
                Note that his list can also be empty.
        """
        res = await self.session.scalars(select(ResultDB).where(ResultDB.artifact_id == artifact_id))
        result_list = [view(m) for m in res.all()]
        return result_list

    async def list_models_by_artifact_id(self, artifact_id: str) -> list[Result]:
        """Get a list of models associated with the given artifact_id. This
        returns all kind of results, both partial and aggregated.

        Args:
            artifact_id (str):
                Id of the artifact to search for.

        Returns:
            Result:
                A list of all the models associated with the given artifact_id.
                Note that his list can also be empty.
        """
        res = await self.session.scalars(
            select(ResultDB).where(
                ResultDB.artifact_id == artifact_id,
                ResultDB.is_model == True,
            )
        )
        result_list = [view(m) for m in res.all()]
        return result_list

    async def list_models(self) -> list[Result]:
        """Returns a list of all the results that are models, partial and aggregated,
        stored in the database.

        Returns:
            list[Result]:
                A list of results. Note that this list can be empty.
        """
        res = await self.session.scalars(
            select(ResultDB).where(
                ResultDB.is_model,
            )
        )
        result_list = [view(r) for r in res.all()]
        return result_list

    async def list_estimations(self) -> list[Result]:
        """Returns al list of all the results that are estimations, partial and
        aggregated, stored in the database.

        Returns:
            list[Result]:
                A list of results. Note that this list can be empty.
        """
        res = await self.session.scalars(
            select(ResultDB).where(
                ResultDB.is_estimation,
            )
        )
        result_list = [view(r) for r in res.all()]
        return result_list

    async def get_aggregated_result(self, artifact_id: str) -> Result:
        """Get the result, considered an aggregated model or estimation, given
        the artifact id.

        Note that for each artifact, only one aggregated result can exists.

        Args:
            artifact_id (str):
                Id of the artifact to get.

        Raises:
            NoResultFound:
                If the result does not exists.

        Returns:
            Result:
                The handler to the result, if one is found.
        """
        res = await self.session.scalars(
            select(ResultDB).where(
                ResultDB.artifact_id == artifact_id,
                ResultDB.is_aggregation == True,
            )
        )
        return view(res.one())

    async def get_partial_result(self, artifact_id: str, client_id: str) -> Result:
        """Get the result, considered as a partial model or estimation, given
        the artifact_id it belongs to and the client_id that produced the result.

        Note that for each pair artifact_id - client_id, only on aggregated
        result can exists.

        Args:
            artifact_id (str):
                Id of the artifact to get.
            client_id (str):
                Id of the client that produced the partial model.

        Raises:
            NoResultFound:
                If the result does not exists.

        Returns:
            Result:
                The handler to the result, if one is found.
        """
        res = await self.session.scalars(
            select(ResultDB).where(
                ResultDB.artifact_id == artifact_id,
                ResultDB.component_id == client_id,
                ResultDB.is_aggregation == False,
            )
        )
        return view(res.one())
