from flotta.cli.suites.jobs.functions import list_jobs
from flotta.const import TYPE_CLIENT
from flotta.database import AsyncSession
from flotta.database.tables import Artifact, Component, Job, Resource
from flotta.schemas.jobs import Job as JobView
from flotta.shared.status import JobStatus

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
    r1: Resource = Resource(
        id="res-1",
        path="",
        component_id="C1",
    )
    j1: Job = Job(
        id="job-1",
        step_id=0,
        artifact_id="A1",
        component_id="C1",
        status=JobStatus.COMPLETED.name,
        path="",
        resource_id="res-1",
    )

    session.add(c1)
    session.add(a1)
    session.add(r1)
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
