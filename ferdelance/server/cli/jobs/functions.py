import pandas as pd

from ....database import DataBase
from ....database.services import JobService
from ....database.tables import Job


async def get_jobs_list(**kwargs) -> pd.DataFrame:
    """Print Job list, with or without filters on ARTIFACT_ID, client_id"""
    artifact_id = kwargs.get("artifact_id", None)
    client_id = kwargs.get("client_id", None)

    db = DataBase()
    async with db.async_session() as session:

        js = JobService(session)

        if artifact_id is not None:
            jobs_session: list[Job] = await js.get_jobs_for_artifact(artifact_id)
        elif client_id is not None:
            jobs_session: list[Job] = await js.get_jobs_for_client(client_id)
        else:
            jobs_session: list[Job] = await js.get_jobs_all()

        jobs_list = [j.dict() for j in jobs_session]

        result: pd.DataFrame = pd.DataFrame(jobs_list)

        print(result)

        return result
