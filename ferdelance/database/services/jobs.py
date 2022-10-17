from ..tables import Job
from .core import DBSessionService, Session

from ferdelance_shared.status import JobStatus

import sqlalchemy.orm.exc as sqlex

from datetime import datetime

import logging

LOGGER = logging.getLogger(__name__)


class JobService(DBSessionService):

    def __init__(self, db: Session) -> None:
        super().__init__(db)

    def schedule_job(self, artifact_id: str, client_id: str) -> Job:
        LOGGER.info(f'scheduled new job for artifact_id={artifact_id} client_id={client_id}')

        job = Job(
            artifact_id=artifact_id,
            client_id=client_id,
            status=JobStatus.SCHEDULED.name
        )

        self.db.add(job)
        self.db.commit()
        self.db.refresh(job)

        return job

    def start_execution(self, job: Job) -> Job:

        # TODO: add checks like in stop_execution method

        self.db.query(Job).filter(Job.job_id == job.job_id).update({
            Job.status: JobStatus.RUNNING.name,
            Job.execution_time: datetime.now(tz=job.creation_time.tzinfo)
        })

        self.db.commit()
        self.db.refresh(job)

        LOGGER.info(f'started execution of job_id={job.job_id} artifact_id={job.artifact_id} client_id={job.client_id}')

        return job

    def stop_execution(self, artifact_id: str, client_id: str) -> Job:
        try:
            job: Job = self.db.query(Job).filter(Job.artifact_id == artifact_id, Job.client_id == client_id, Job.status == JobStatus.RUNNING.name).one()

            self.db.query(Job).filter(Job.job_id == job.job_id).update({
                Job.status: JobStatus.COMPLETED.name,
                Job.termination_time: datetime.now(tz=job.creation_time.tzinfo)
            })

            self.db.commit()
            self.db.refresh(job)

            LOGGER.info(f'completed execution of job_id={job.job_id} artifact_id={job.artifact_id} client_id={job.client_id}')

            return job

        except sqlex.NoResultFound:
            LOGGER.error(f'Could not terminate a job that does not exists or has not started yet with artifact_id={artifact_id} client_id={client_id}')
            raise ValueError(f'Job in status RUNNING not found for artifact_id={artifact_id} client_id={client_id}')

        except sqlex.MultipleResultsFound:
            LOGGER.error(f'Multiple jobs have been started for artifact_id={artifact_id} client_id={client_id}')
            raise ValueError(f'Multiple job in status RUNNING found for artifact_id={artifact_id} client_id={client_id}')

    def error(self, job: Job) -> Job:

        # TODO: add checks like in stop_execution method

        self.db.query(Job).filter(Job.job_id == job.job_id).update({
            Job.status: JobStatus.ERROR.name,
            Job.termination_time: datetime.now(tz=job.creation_time.tzinfo)
        })

        self.db.commit()
        self.db.refresh(job)

        LOGGER.error(f'failed execution of job_id={job.job_id} artifact_id={job.artifact_id} client_id={job.client_id}')

        return job

    def get_jobs_for_client(self, client_id: str) -> list[Job]:
        return self.db.query(Job).filter(Job.client_id == client_id).all()

    def get_jobs_all(self) -> list[Job]:
        return self.db.query(Job).all()

    def get_jobs_for_artifact(self, artifact_id: str) -> list[Job]:
        return self.db.query(Job).filter(Job.artifact_id == artifact_id).all()

    def count_jobs_by_status(self, artifact_id: str, status: JobStatus) -> int:
        return self.db.query(Job).filter(Job.artifact_id == artifact_id, Job.status == status.name).count()

    def next_job_for_client(self, client_id: str) -> Job | None:
        return self.db.query(Job)\
            .filter(
                Job.client_id == client_id,
                Job.status == JobStatus.SCHEDULED.name
        )\
            .order_by(Job.creation_time.asc())\
            .first()
