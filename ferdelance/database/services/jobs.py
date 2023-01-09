from ..tables import Job
from .core import DBSessionService, AsyncSession

from ferdelance.shared.status import JobStatus

from sqlalchemy.exc import MultipleResultsFound, NoResultFound
from sqlalchemy import select, func

from datetime import datetime

import logging

LOGGER = logging.getLogger(__name__)


class JobService(DBSessionService):

    def __init__(self, session: AsyncSession) -> None:
        super().__init__(session)

    async def schedule_job(self, artifact_id: str, client_id: str) -> Job:
        LOGGER.info(f'client_id={client_id}: scheduled new job for artifact_id={artifact_id}')

        job = Job(
            artifact_id=artifact_id,
            client_id=client_id,
            status=JobStatus.SCHEDULED.name
        )

        self.session.add(job)
        await self.session.commit()
        await self.session.refresh(job)

        return job

    async def start_execution(self, job: Job) -> Job:

        # TODO: add checks like in stop_execution method

        stmt = select(Job).where(Job.job_id == job.job_id)
        job_db: Job = await self.session.scalar(stmt)

        job_db.status = JobStatus.RUNNING.name
        job_db.execution_time = datetime.now(tz=job.creation_time.tzinfo)

        await self.session.commit()
        await self.session.refresh(job)

        LOGGER.info(f'client_id={job.client_id}: started execution of job_id={job.job_id} artifact_id={job.artifact_id}')

        return job

    async def stop_execution(self, artifact_id: str, client_id: str) -> Job:
        try:
            stmt = select(Job).where(Job.artifact_id == artifact_id, Job.client_id == client_id, Job.status == JobStatus.RUNNING.name)

            res = await self.session.execute(stmt)
            job: Job = res.scalar_one()

            job.status = JobStatus.COMPLETED.name
            job.termination_time = datetime.now(tz=job.creation_time.tzinfo)

            await self.session.commit()
            await self.session.refresh(job)

            LOGGER.info(f'client_id={job.client_id}: completed execution of job_id={job.job_id} artifact_id={job.artifact_id}')

            return job

        except NoResultFound:
            LOGGER.error(f'Could not terminate a job that does not exists or has not started yet with artifact_id={artifact_id} client_id={client_id}')
            raise ValueError(f'Job in status RUNNING not found for artifact_id={artifact_id} client_id={client_id}')

        except MultipleResultsFound:
            LOGGER.error(f'Multiple jobs have been started for artifact_id={artifact_id} client_id={client_id}')
            raise ValueError(f'Multiple job in status RUNNING found for artifact_id={artifact_id} client_id={client_id}')

    async def error(self, job: Job) -> Job:

        # TODO: add checks like in stop_execution method

        stmt = select(Job).where(Job.job_id == job.job_id)
        job_db: Job = await self.session.scalar(stmt)

        job_db.status = JobStatus.ERROR.name,
        job_db.termination_time = datetime.now(tz=job.creation_time.tzinfo)

        await self.session.commit()
        await self.session.refresh(job)

        LOGGER.error(f'client_id={job.client_id}: failed execution of job_id={job.job_id} artifact_id={job.artifact_id}')

        return job

    async def get_jobs_for_client(self, client_id: str) -> list[Job]:
        res = await self.session.scalars(select(Job).where(Job.client_id == client_id))
        return res.all()

    async def get_jobs_all(self) -> list[Job]:
        res = await self.session.scalars(select(Job))
        return res.all()

    async def get_jobs_for_artifact(self, artifact_id: str) -> list[Job]:
        res = await self.session.scalars(select(Job).where(Job.artifact_id == artifact_id))
        return res.all()

    async def count_jobs_for_artifact(self, artifact_id: str) -> int:
        return await self.session.scalar(
            select(func.count())
            .select_from(Job)
            .where(Job.artifact_id == artifact_id)
        )

    async def count_jobs_by_status(self, artifact_id: str, status: JobStatus) -> int:
        return await self.session.scalar(
            select(func.count())
            .select_from(Job)
            .where(
                Job.artifact_id == artifact_id,
                Job.status == status.name
            )
        )

    async def next_job_for_client(self, client_id: str) -> Job | None:
        ret = await self.session.execute(
            select(Job)
            .where(
                Job.client_id == client_id,
                Job.status == JobStatus.SCHEDULED.name
            )
            .order_by(Job.creation_time.asc())
            .limit(1)
        )
        return ret.scalar_one_or_none()
