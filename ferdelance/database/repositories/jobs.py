from ferdelance.logging import get_logger
from ferdelance.database.tables import Job as JobDB
from ferdelance.database.repositories.core import AsyncSession, Repository
from ferdelance.schemas.jobs import Job
from ferdelance.shared.status import JobStatus

from sqlalchemy import func, select
from sqlalchemy.exc import MultipleResultsFound, NoResultFound

from datetime import datetime
from uuid import uuid4


LOGGER = get_logger(__name__)


def view(job: JobDB) -> Job:
    return Job(
        id=job.id,
        artifact_id=job.artifact_id,
        component_id=job.component_id,
        status=job.status,
        creation_time=job.creation_time,
        execution_time=job.execution_time,
        termination_time=job.termination_time,
        is_model=job.is_model,
        is_estimation=job.is_estimation,
        is_aggregation=job.is_aggregation,
        iteration=job.iteration,
    )


class JobRepository(Repository):
    """A repository used to manage and store jobs.

    Jobs are an alternate term for Task. Everything that is submitted and need
    to be processed is a job. When a client ask for update it can receive a new
    job to execute."""

    def __init__(self, session: AsyncSession) -> None:
        super().__init__(session)

    async def schedule_job(
        self,
        artifact_id: str,
        component_id: str,
        is_model: bool = False,
        is_estimation: bool = False,
        is_aggregation: bool = False,
        iteration: int = 0,
    ) -> Job:
        """Starts a job by inserting it into the database. The insertion works
        as a scheduling the new work to do. The initial state of the job will
        be JobStatus.SCHEDULED.

        Args:
            artifact_id (str):
                Id of the artifact to schedule.
            component_id (str):
                Id of the client that will have to execute the job.
            is_model (bool, optional):
                If true, the job will be considered as a training of a new model.
                Defaults to False.
            is_estimation (bool, optional):
                If true, the job will be considered as an estimation.
                Defaults to False.
            is_aggregation (bool, optional):
                If true, the job will be considered as an aggregation, otherwise
                a partial (local) job. Defaults to False.

        Returns:
            Job:
                An handler to the scheduled job.
        """
        LOGGER.info(
            f"component_id={component_id}: scheduling new job for artifact_id={artifact_id} iteration={iteration}"
        )

        # TODO: what happen if we submit again the same artifact?

        job = JobDB(
            id=str(uuid4()),
            artifact_id=artifact_id,
            component_id=component_id,
            status=JobStatus.SCHEDULED.name,
            is_model=is_model,
            is_estimation=is_estimation,
            is_aggregation=is_aggregation,
            iteration=iteration,
        )

        self.session.add(job)
        await self.session.commit()
        await self.session.refresh(job)

        return view(job)

    async def start_execution(self, job: Job) -> Job:
        """Changes the state of the given job to JobStatus.RUNNING. An exception
        is raised if the job is not in JobStatus.SCHEDULED state, or if the job
        does not exists.

        Args:
            job (Job):
                Handler of the job to start.

        Raises:
            ValueError:
                If the job does not exists in the SCHEDULED state.

        Returns:
            Job:
                Updated handler of the started job.
        """

        job_id: str = job.id
        artifact_id: str = job.artifact_id
        component_id: str = job.component_id

        try:
            res = await self.session.scalars(
                select(JobDB).where(
                    JobDB.id == job.id,
                    JobDB.status == JobStatus.SCHEDULED.name,
                )
            )
            job_db: JobDB = res.one()

            job_db.status = JobStatus.RUNNING.name
            job_db.execution_time = datetime.now(tz=job.creation_time.tzinfo)

            await self.session.commit()
            await self.session.refresh(job_db)

            LOGGER.info(
                f"component_id={job_db.component_id}: started execution of artifact_id={artifact_id} job_id={job_id}"
            )

            return view(job_db)

        except NoResultFound:
            LOGGER.error(
                f"job_id={job_id}: Could not start a job that does not exists in the SCHEDULED state with "
                f"artifact_id={artifact_id} component_id={component_id}"
            )
            raise ValueError(
                f"Job in status SCHEDULED not found for job_id={job_id} "
                f"artifact_id={artifact_id} component_id={component_id}"
            )

    async def mark_completed(self, job_id: str, component_id: str) -> Job:
        """Changes the state of a job to JobStatus.COMPLETED. The job is identified
        by the artifact_id and the component_id that have completed the required
        operations. An exception is raised if there are no job in the JobStatus.RUNNING
        state or if there are multiple jobs available (this should never happen).

        Args:
            artifact_id (str):
                Id of the artifact that has been completed.
            component_id (str):
                Id of the component that has completed the job.

        Raises:
            ValueError:
                If there are no jobs in the RUNNING state given the input arguments.
            ValueError:
                If there are multiple jobs in the correct state given the input arguments.

        Returns:
            Job:
                Updated handler of the job.
        """
        try:
            res = await self.session.scalars(
                select(JobDB).where(
                    JobDB.id == job_id,
                    JobDB.status == JobStatus.RUNNING.name,
                )
            )
            job: JobDB = res.one()

            job.status = JobStatus.COMPLETED.name
            job.termination_time = datetime.now(tz=job.creation_time.tzinfo)

            await self.session.commit()
            await self.session.refresh(job)

            LOGGER.info(
                f"component_id={job.component_id}: completed execution of job_id={job.id} artifact_id={job.artifact_id}"
            )

            return view(job)

        except NoResultFound:
            LOGGER.error(
                f"Could not terminate a job that does not exists or has not "
                f"started yet with job_id={job_id} component_id={component_id}"
            )
            raise ValueError(f"Job in status RUNNING not found for job_id={job_id} component_id={component_id}")

        except MultipleResultsFound:
            LOGGER.error(f"Multiple jobs have been started for job_id={job_id} component_id={component_id}")
            raise ValueError(f"Multiple job in status RUNNING found for job_id={job_id} component_id={component_id}")

    async def mark_error(self, job_id: str, component_id: str) -> Job:
        """Changes the state of a job to JobStatus.ERROR. The job is identified
        by the job_id given in the handler. An exception is raised if no jobs
        are found.

        Args:
            artifact_id (str):
                Id of the artifact that has been completed.
            component_id (str):
                Id of the component that has completed the job.

        Raises:
            ValueError:
                If no job has been found.

        Returns:
            Job:
                Updated handler of the job.
        """

        try:
            res = await self.session.scalars(
                select(JobDB).where(
                    JobDB.id == job_id,
                    JobDB.status == JobStatus.RUNNING.name,
                )
            )
            job: JobDB = res.one()

            job.status = JobStatus.ERROR.name
            job.termination_time = datetime.now(tz=job.creation_time.tzinfo)

            await self.session.commit()
            await self.session.refresh(job)

            LOGGER.warn(
                f"component_id={job.component_id}: failed execution of job_id={job.id} "
                f"artifact_id={job.artifact_id} component_id={component_id}"
            )

            return view(job)

        except NoResultFound:
            LOGGER.error(
                f"component_id={component_id}: could not mark error a job that does not exists with job_id={job_id} "
            )
            raise ValueError(f"Job not found with job_id={job_id} component_id={component_id}")

    async def get_by_id(self, job_id: str) -> Job:
        """Gets the data on the job associated with the given job_id.

        Args:
            job_id (str):
                Id of the job to retrieve.

        Raises:
            NoResultsFound:
                If the job does not exists.

        Returns:
            Job:
                The handler of the job.
        """
        res = await self.session.scalars(select(JobDB).where(JobDB.id == job_id))
        return view(res.one())

    async def get(self, job: Job) -> Job:
        """Gets an updated version of the given job.

        Args:
            job (Job):
                Handler of the job.

        Raises:
            NoResultsFound:
                If the job does not exists.

        Returns:
            Job:
                Updated handler of the job.
        """
        res = await self.session.scalars(select(JobDB).where(JobDB.id == job.id))
        return view(res.one())

    async def list_jobs_by_component_id(self, component_id: str) -> list[Job]:
        """Returns a list of jobs assigned to the given component_id.

        Args:
            component_id (str):
                Id of the component to list for.

        Returns:
            list[Job]:
                A list of job handlers assigned to the given component. Note
                that this list can be an empty list.
        """
        res = await self.session.scalars(select(JobDB).where(JobDB.component_id == component_id))
        return [view(j) for j in res.all()]

    async def list_jobs_by_status(self, status: JobStatus) -> list[Job]:
        """Returns a list of all jobs with the given status.

        Args:
            status (JobStatus):
                The status to search for.

        Returns:
            list[Job]:
                A list of job handlers with the given status. Note that this list
                can be an empty list.
        """
        res = await self.session.scalars(select(JobDB).where(JobDB.status == status.name))
        return [view(j) for j in res.all()]

    async def list_jobs(self) -> list[Job]:
        """Returns all jobs in the database.

        Returns:
            list[Job]:
                A list of job handlers. Note that this list can be an empty list.
        """
        res = await self.session.scalars(select(JobDB))
        job_list = [view(j) for j in res.all()]
        return job_list

    async def list_jobs_by_artifact_id(self, artifact_id: str) -> list[Job]:
        """Returns a list of jobs created for the given artifact_id.

        Args:
            artifact_id (str):
                Id of the artifact to list for.

        Returns:
            list[Job]:
                A list of job handlers created by the given artifact. Note that
                this list can be an empty list.
        """
        res = await self.session.scalars(select(JobDB).where(JobDB.artifact_id == artifact_id))
        return [view(j) for j in res.all()]

    async def count_jobs_by_artifact_id(self, artifact_id: str, iteration: int = -1) -> int:
        """Counts the number of jobs created for the given artifact_id.

        Args:
            artifact_id (str):
                Id of the artifact to count for.
            iteration (int):
                If greater than -1, count only for the given iteration.

        Returns:
            int:
                The number, greater than zero, of jobs created.
        """
        conditions = [JobDB.artifact_id == artifact_id]
        if iteration > -1:
            conditions.append(JobDB.iteration == iteration)

        res = await self.session.scalars(select(func.count()).select_from(JobDB).where(*conditions))
        return res.one()

    async def count_jobs_by_artifact_status(self, artifact_id: str, status: JobStatus, iteration: int = -1) -> int:
        """Counts the number of jobs created for the given artifact_id and in
        the given status.

        Args:
            artifact_id (str):
                Id of the artifact to count for.
            status (JobStatus):
                Desired status of the jobs.

        Returns:
            int:
                The number, greater than zero, of jobs in the given state.
        """
        conditions = [JobDB.artifact_id == artifact_id, JobDB.status == status.name]
        if iteration > -1:
            conditions.append(JobDB.iteration == iteration)

        res = await self.session.scalars(select(func.count()).select_from(JobDB).where(*conditions))
        return res.one()

    async def next_job_for_component(self, component_id: str) -> Job:
        """Check the database for the next job for the given component. The
        next job is the oldest job in the SCHEDULED state.

        Args:
            component_id (str):
                Id of the component to search for.

        Raises:
            NoResultFound:
                If there are no more jobs for the component.

        Returns:
            Job:
                The next available job.
        """
        ret = await self.session.scalars(
            select(JobDB)
            .where(JobDB.component_id == component_id, JobDB.status == JobStatus.SCHEDULED.name)
            .order_by(JobDB.creation_time.asc())
            .limit(1)
        )
        return view(ret.one())
