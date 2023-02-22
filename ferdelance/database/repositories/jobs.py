from ferdelance.database.tables import Job as JobDB
from ferdelance.database.repositories.core import AsyncSession, DBSessionService
from ferdelance.schemas.jobs import Job
from ferdelance.shared.status import JobStatus

from sqlalchemy import func, select
from sqlalchemy.exc import MultipleResultsFound, NoResultFound

from datetime import datetime

import logging

LOGGER = logging.getLogger(__name__)


def view(job: JobDB) -> Job:
    return Job(
        job_id=job.job_id,
        artifact_id=job.artifact_id,
        client_id=job.component_id,
        status=job.status,
        creation_time=job.creation_time,
        execution_time=job.execution_time,
        termination_time=job.termination_time,
    )


class JobService(DBSessionService):
    def __init__(self, session: AsyncSession) -> None:
        super().__init__(session)

    async def schedule_job(self, artifact_id: str, client_id: str) -> Job:
        LOGGER.info(f"component_id={client_id}: scheduled new job for artifact_id={artifact_id}")

        job = JobDB(
            artifact_id=artifact_id,
            component_id=client_id,
            status=JobStatus.SCHEDULED.name,
        )

        self.session.add(job)
        await self.session.commit()
        await self.session.refresh(job)

        return view(job)

    async def start_execution(self, job: Job) -> Job:
        """Can raise NoResultException."""

        # TODO: add checks like in stop_execution method

        stmt = select(JobDB).where(JobDB.job_id == job.job_id)
        res = await self.session.scalars(stmt)
        job_db: JobDB = res.one()

        job_db.status = JobStatus.RUNNING.name
        job_db.execution_time = datetime.now(tz=job.creation_time.tzinfo)

        await self.session.commit()
        await self.session.refresh(job_db)

        LOGGER.info(
            f"component_id={job_db.component_id}: started execution of job_id={job_db.job_id} artifact_id={job_db.artifact_id}"
        )

        return view(job_db)

    async def stop_execution(self, artifact_id: str, client_id: str) -> Job:
        try:
            stmt = select(JobDB).where(
                JobDB.artifact_id == artifact_id,
                JobDB.component_id == client_id,
                JobDB.status == JobStatus.RUNNING.name,
            )

            res = await self.session.execute(stmt)
            job: JobDB = res.scalar_one()

            job.status = JobStatus.COMPLETED.name
            job.termination_time = datetime.now(tz=job.creation_time.tzinfo)

            await self.session.commit()
            await self.session.refresh(job)

            LOGGER.info(
                f"client_id={job.component_id}: completed execution of job_id={job.job_id} artifact_id={job.artifact_id}"
            )

            return view(job)

        except NoResultFound:
            LOGGER.error(
                f"Could not terminate a job that does not exists or has not started yet with artifact_id={artifact_id} client_id={client_id}"
            )
            raise ValueError(f"Job in status RUNNING not found for artifact_id={artifact_id} client_id={client_id}")

        except MultipleResultsFound:
            LOGGER.error(f"Multiple jobs have been started for artifact_id={artifact_id} client_id={client_id}")
            raise ValueError(
                f"Multiple job in status RUNNING found for artifact_id={artifact_id} client_id={client_id}"
            )

    async def error(self, job: Job) -> Job:
        """Can raise NoResultException."""

        # TODO: add checks like in stop_execution method

        res = await self.session.scalars(select(JobDB).where(JobDB.job_id == job.job_id))
        job_db: JobDB = res.one()

        job_db.status = JobStatus.ERROR.name
        job_db.termination_time = datetime.now(tz=job.creation_time.tzinfo)

        await self.session.commit()
        await self.session.refresh(job)

        LOGGER.error(
            f"client_id={job_db.component_id}: failed execution of job_id={job_db.job_id} artifact_id={job_db.artifact_id}"
        )

        return view(job_db)

    async def get(self, job: Job) -> Job:
        """Can raise NoResultFound."""
        res = await self.session.execute(select(JobDB).where(JobDB.job_id == job.job_id))
        return view(res.scalar_one())

    async def get_jobs_for_client(self, client_id: str) -> list[Job]:
        res = await self.session.scalars(select(JobDB).where(JobDB.component_id == client_id))
        return [view(j) for j in res.all()]

    async def get_jobs_all(self) -> list[Job]:
        res = await self.session.scalars(select(JobDB))
        job_list = [view(j) for j in res.all()]
        return job_list

    async def get_jobs_for_artifact(self, artifact_id: str) -> list[Job]:
        res = await self.session.scalars(select(JobDB).where(JobDB.artifact_id == artifact_id))
        return [view(j) for j in res.all()]

    async def count_jobs_for_artifact(self, artifact_id: str) -> int:
        res = await self.session.scalars(
            select(func.count()).select_from(JobDB).where(JobDB.artifact_id == artifact_id)
        )
        return res.one()

    async def count_jobs_by_status(self, artifact_id: str, status: JobStatus) -> int:
        res = await self.session.scalars(
            select(func.count()).select_from(JobDB).where(JobDB.artifact_id == artifact_id, JobDB.status == status.name)
        )
        return res.one()

    async def next_job_for_client(self, client_id: str) -> Job:
        """Can raise NoResultFound."""
        ret = await self.session.execute(
            select(JobDB)
            .where(JobDB.component_id == client_id, JobDB.status == JobStatus.SCHEDULED.name)
            .order_by(JobDB.creation_time.asc())
            .limit(1)
        )

        return view(ret.scalar_one())
