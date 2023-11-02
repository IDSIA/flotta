from ferdelance.database.tables import JobLock as JobLockDB, Job as JobDB
from ferdelance.database.repositories import JobRepository, ArtifactRepository
from ferdelance.node.api import api
from ferdelance.core.artifacts import Artifact
from ferdelance.shared.exchange import Exchange

from tests.utils import create_project, create_node

from fastapi.testclient import TestClient

from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession

import pytest


@pytest.mark.asyncio
async def test_job_change_status(session: AsyncSession, exchange: Exchange):
    with TestClient(api) as client:
        ar = ArtifactRepository(session)
        jr = JobRepository(session)

        p_token: str = "123456789"

        await create_project(session, p_token)
        client_id = create_node(client, exchange)

        a = Artifact(
            id="artifact",
            project_id=p_token,
            steps=[],
        )

        await ar.create_artifact(a)

        job0 = await jr.create_job(a.id, client_id, "", job_id="job0")
        job1 = await jr.create_job(a.id, client_id, "", job_id="job1")
        job2 = await jr.create_job(a.id, client_id, "", job_id="job2")
        job3 = await jr.create_job(a.id, client_id, "", job_id="job3")
        job4 = await jr.create_job(a.id, client_id, "", job_id="job4")
        job5 = await jr.create_job(a.id, client_id, "", job_id="job5")
        job6 = await jr.create_job(a.id, client_id, "", job_id="job6")
        job7 = await jr.create_job(a.id, client_id, "", job_id="job7")
        job8 = await jr.create_job(a.id, client_id, "", job_id="job8")

        await jr.add_locks(job0, [job1, job2, job3])
        await jr.add_locks(job1, [job4])
        await jr.add_locks(job2, [job4])
        await jr.add_locks(job3, [job4])
        await jr.add_locks(job4, [job5, job6, job7])
        await jr.add_locks(job5, [job8])
        await jr.add_locks(job6, [job8])
        await jr.add_locks(job7, [job8])

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
            jobs = await jr.list_unlocked_jobs(a.id)

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
