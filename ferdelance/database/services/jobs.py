import logging
from datetime import datetime

from sqlalchemy import func, select
from sqlalchemy.exc import MultipleResultsFound, NoResultFound

from ferdelance.shared.status import JobStatus

from ..schemas import Job as JobView
from ..tables import Job as JobDB
from .core import AsyncSession, DBSessionService

LOGGER = logging.getLogger(__name__)


def get_view(job: JobDB) -> JobView:
    return JobView(**job.__dict__)


class JobService(DBSessionService):
    def __init__(self, session: AsyncSession) -> None:
        super().__init__(session)

    async def schedule_job(self, artifact_id: str, client_id: str) -> JobDB:
        LOGGER.info(
            f"client_id={client_id}: scheduled new job for artifact_id={artifact_id}"
        )

        job = JobDB(
            artifact_id=artifact_id,
            client_id=client_id,
            status=JobStatus.SCHEDULED.name,
        )

        self.session.add(job)
        await self.session.commit()
        await self.session.refresh(job)

        return job

    async def start_execution(self, job: JobDB) -> JobView:

        # TODO: add checks like in stop_execution method

        stmt = select(JobDB).where(JobDB.job_id == job.job_id)
        job_db: JobDB = await self.session.scalar(stmt)

        job_db.status = JobStatus.RUNNING.name
        job_db.execution_time = datetime.now(tz=job.creation_time.tzinfo)

        await self.session.commit()
        await self.session.refresh(job)

        LOGGER.info(
            f"client_id={job.client_id}: started execution of job_id={job.job_id} artifact_id={job.artifact_id}"
        )

        return get_view(job)

    async def stop_execution(self, artifact_id: str, client_id: str) -> JobView:
        try:
            stmt = select(JobDB).where(
                JobDB.artifact_id == artifact_id,
                JobDB.client_id == client_id,
                JobDB.status == JobStatus.RUNNING.name,
            )

            res = await self.session.execute(stmt)
            job: JobDB = res.scalar_one()

            job.status = JobStatus.COMPLETED.name
            job.termination_time = datetime.now(tz=job.creation_time.tzinfo)

            await self.session.commit()
            await self.session.refresh(job)

            LOGGER.info(
                f"client_id={job.client_id}: completed execution of job_id={job.job_id} artifact_id={job.artifact_id}"
            )

            return get_view(job)

        except NoResultFound:
            LOGGER.error(
                f"Could not terminate a job that does not exists or has not started yet with artifact_id={artifact_id} client_id={client_id}"
            )
            raise ValueError(
                f"Job in status RUNNING not found for artifact_id={artifact_id} client_id={client_id}"
            )

        except MultipleResultsFound:
            LOGGER.error(
                f"Multiple jobs have been started for artifact_id={artifact_id} client_id={client_id}"
            )
            raise ValueError(
                f"Multiple job in status RUNNING found for artifact_id={artifact_id} client_id={client_id}"
            )

    async def error(self, job: JobDB) -> JobDB:

        # TODO: add checks like in stop_execution method

        stmt = select(JobDB).where(JobDB.job_id == job.job_id)
        job_db: JobDB = await self.session.scalar(stmt)

        job_db.status = (JobStatus.ERROR.name,)
        job_db.termination_time = datetime.now(tz=job.creation_time.tzinfo)

        await self.session.commit()
        await self.session.refresh(job)

        LOGGER.error(
            f"client_id={job.client_id}: failed execution of job_id={job.job_id} artifact_id={job.artifact_id}"
        )

        return get_view(job)

    async def get_jobs_for_client(self, client_id: str) -> list[JobView]:
        res = await self.session.scalars(
            select(JobDB).where(JobDB.client_id == client_id)
        )
        job_list = [get_view(j) for j in res.all()]
        return job_list

    async def get_jobs_all(self) -> list[JobView]:
        res = await self.session.scalars(select(JobDB))
        job_list = [get_view(j) for j in res.all()]
        return job_list

    async def get_jobs_for_artifact(self, artifact_id: str) -> list[JobView]:
        res = await self.session.scalars(
            select(JobDB).where(JobDB.artifact_id == artifact_id)
        )
        job_list = [get_view(j) for j in res.all()]
        return job_list

    async def count_jobs_for_artifact(self, artifact_id: str) -> int:
        return await self.session.scalar(
            select(func.count())
            .select_from(JobDB)
            .where(JobDB.artifact_id == artifact_id)
        )

    async def count_jobs_by_status(self, artifact_id: str, status: JobStatus) -> int:
        return await self.session.scalar(
            select(func.count())
            .select_from(JobDB)
            .where(JobDB.artifact_id == artifact_id, JobDB.status == status.name)
        )

    async def next_job_for_client(self, client_id: str) -> JobDB | None:
        ret = await self.session.execute(
            select(JobDB)
            .where(
                JobDB.client_id == client_id, JobDB.status == JobStatus.SCHEDULED.name
            )
            .order_by(JobDB.creation_time.asc())
            .limit(1)
        )
        return ret.scalar_one_or_none()
