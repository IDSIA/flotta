from ferdelance.cli.fdl_suites.jobs.functions import list_jobs
from ferdelance.database import AsyncSession
from ferdelance.schemas.jobs import Job as JobView
from ferdelance.database.tables import Artifact, Component, Job

import pytest


@pytest.mark.asyncio
async def test_jobs_list(session: AsyncSession):
    c1: Component = Component(
        id="C1",
        version="test",
        public_key="1",
        machine_system="1",
        machine_mac_address="1",
        machine_node="1",
        ip_address="1",
        type_name="CLIENT",
    )
    a1: Artifact = Artifact(
        id="A1",
        path="test-path",
        status="S1",
    )
    j1: Job = Job(
        id="job-1",
        artifact_id="A1",
        component_id="C1",
        status="J1",
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
