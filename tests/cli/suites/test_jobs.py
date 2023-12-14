from ferdelance.cli.fdl_suites.jobs.functions import list_jobs
from ferdelance.const import TYPE_CLIENT
from ferdelance.database import AsyncSession
from ferdelance.database.tables import Artifact, Component, Job
from ferdelance.schemas.jobs import Job as JobView
from ferdelance.shared.status import JobStatus

import pytest


@pytest.mark.asyncio
async def test_jobs_list(session: AsyncSession):
    c1: Component = Component(
        id="C1",
        name="client1",
        version="test",
        public_key="1",
        ip_address="1",
        url="",
        type_name=TYPE_CLIENT,
    )
    a1: Artifact = Artifact(
        id="A1",
        path="test-path",
        status="S1",
    )
    j1: Job = Job(
        id="job-1",
        step_id=0,
        artifact_id="A1",
        component_id="C1",
        status=JobStatus.COMPLETED.name,
        path="",
    )

    session.add(c1)
    session.add(a1)
    session.add(j1)

    await session.commit()

    res: list[JobView] = await list_jobs()

    assert len(res) == 1

    res: list[JobView] = await list_jobs(artifact_id="A1")

    assert len(res) == 1

    res: list[JobView] = await list_jobs(client_id="C1")

    assert len(res) == 1

    res: list[JobView] = await list_jobs(client_id="C7")

    assert len(res) == 0
