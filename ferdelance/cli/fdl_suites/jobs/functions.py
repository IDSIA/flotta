from ferdelance.cli.visualization import show_many
from ferdelance.database import DataBase
from ferdelance.database.repositories import JobRepository
from ferdelance.schemas.jobs import Job


async def list_jobs(artifact_id: str | None = None, client_id: str | None = None) -> list[Job]:
    """Print and return Job List, with or without filters on ARTIFACT_ID, client_id

    Args:
        artifact_id (str, optional): Filter by artifact. Defaults to None.
        client_id (str, optional): Filter by client. Defaults to None.

    Returns:
        List[Job]: List of Job objects
    """

    db = DataBase()

    async with db.async_session() as session:
        jr = JobRepository(session)

        if artifact_id is not None:
            jobs: list[Job] = await jr.list_jobs_by_artifact_id(artifact_id)
        elif client_id is not None:
            jobs: list[Job] = await jr.list_jobs_by_component_id(client_id)
        else:
            jobs: list[Job] = await jr.list_jobs()

        show_many(jobs)

        return jobs
