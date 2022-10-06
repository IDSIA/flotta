from .core import DBSessionService, Session

from ..tables import Job

from ferdelance_shared.status import JobStatus

from uuid import uuid4


class JobService(DBSessionService):

    def __init__(self, db: Session) -> None:
        super().__init__(db)

    def create_job(self, artifact_id: str, client_id: str, status: JobStatus) -> Job:

        job = Job(
            job_id=str(uuid4()),
            artifact_id=artifact_id,
            client_id=client_id,
            status=status.name
        )

        self.db.add(job)
        self.db.commit()
        self.db.refresh(job)

        return job

    def get_jobs_for_client(self, client_id: str) -> list[Job]:
        return self.db.query(Job).filter(Job.client_id == client_id).all()

    def get_jobs_for_artifact(self, artifact_id: str) -> list[Job]:
        return self.db.query(Job).filter(Job.artifact_id == artifact_id).all()

    def get_job_by_id(self, job_id: str) -> Job | None:
        return self.db.query(Job).filter(Job.job_id == job_id).order_by(Job.time.asc()).first()

    def get_job_for_client(self, client_id: str) -> Job | None:
        return self.db.query(Job).filter(Job.status == JobStatus.SCHEDULED.name, Job.client_id == client_id).order_by(Job.time.asc()).first()
