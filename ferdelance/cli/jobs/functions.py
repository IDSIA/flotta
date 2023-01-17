import pandas as pd

from ...database import DataBase
from ...database.schemas import Job
from ...database.services import JobService


async def get_jobs_list(artifact_id=None, client_id=None) -> pd.DataFrame:
    """Print Job list, with or without filters on ARTIFACT_ID, client_id"""

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
