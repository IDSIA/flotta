from ferdelance.core.artifacts import Artifact
from ferdelance.core.distributions import Collect, Distribute
from ferdelance.core.interfaces import SchedulerContext
from ferdelance.core.steps import Finalize, Initialize, Parallel
from ferdelance.database.tables import JobLock as JobLockDB, Job as JobDB
from ferdelance.database.repositories import JobRepository, ArtifactRepository
from ferdelance.node.api import api
from ferdelance.schemas.components import Component
from ferdelance.schemas.jobs import Job

from tests.utils import create_project, create_node, setup_exchange
from tests.dummies import DummyOp

from fastapi.testclient import TestClient

from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession

import pytest


@pytest.mark.asyncio
async def test_job_change_status(session: AsyncSession):
    with TestClient(api) as client:
        ar = ArtifactRepository(session)
        jr = JobRepository(session)

        p_token: str = "123456789"

        await create_project(session, p_token)
        node = create_node(client, setup_exchange())
        worker1 = create_node(client, setup_exchange())
        worker2 = create_node(client, setup_exchange())
        worker3 = create_node(client, setup_exchange())

        a = Artifact(
            id="artifact",
            project_id=p_token,
            steps=[
                Initialize(DummyOp(), Distribute()),
                Parallel(DummyOp(), Collect()),
                Finalize(DummyOp(), Distribute()),
                Parallel(DummyOp(), Collect()),
                Finalize(DummyOp()),
            ],
        )

        await ar.create_artifact(a)

        jobs = a.jobs(
            SchedulerContext(
                artifact_id=a.id,
                initiator=Component(id=node, type_name="node", public_key=""),
                workers=[
                    Component(id=worker1, type_name="node", public_key=""),
                    Component(id=worker2, type_name="node", public_key=""),
                    Component(id=worker3, type_name="node", public_key=""),
                ],
            )
        )

        assert len(jobs) == 9

        assert jobs[0].locks == [1, 2, 3]
        assert jobs[1].locks == [4]
        assert jobs[2].locks == [4]
        assert jobs[3].locks == [4]
        assert jobs[4].locks == [5, 6, 7]
        assert jobs[5].locks == [8]
        assert jobs[6].locks == [8]
        assert jobs[7].locks == [8]
        assert jobs[8].locks == []

        job_map: dict[int, Job] = dict()

        for i, job in enumerate(jobs):
            j = await jr.create_job(a.id, job, job_id=f"job{i}")
            job_map[job.id] = j

        for job in jobs:
            j = job_map[job.id]
            unlocks = [job_map[i] for i in job.locks]

            await jr.add_locks(j, unlocks)

        job0 = job_map[0]
        job1 = job_map[1]
        job2 = job_map[2]
        job3 = job_map[3]

        n_jobs = await session.scalar(select(func.count()).select_from(JobDB))
        assert n_jobs == 9

        n_locks = await session.scalar(select(func.count()).select_from(JobLockDB))
        assert n_locks == 12

        async def list_jobs():
            jobs = await jr.list_jobs()

            print("list all jobs")
            for j in jobs:
                print("job: id=", j.id)
            print()

        async def list_unlocks():
            print("list all unlocks")
            unlocks = await session.scalars(select(JobLockDB))

            print("all locks")
            for u in unlocks.all():
                print(f"unlock: id={u.id:2} job_id={u.job_id} next_job={u.next_id} locked={u.locked}")
            print()

        async def list_unlocked_jobs():
            print("list unlocked jobs")
            jobs = await jr.list_unlocked_jobs_by_artifact_id(a.id)

            for job in jobs:
                print("job: id=", job.id)
            print()

        async def unlock(job):
            print(f"unlock {job.id}")
            await jr.unlock_job(job)

        await list_jobs()
        await list_unlocks()
        await list_unlocked_jobs()

        await unlock(job0)

        await list_unlocked_jobs()
        await list_unlocks()

        await unlock(job1)
        await unlock(job2)

        await list_unlocked_jobs()

        await unlock(job3)

        await list_unlocked_jobs()
