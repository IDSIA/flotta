import pandas as pd
import pytest

from ferdelance.cli.suites.jobs.functions import list_jobs
from ferdelance.database import AsyncSession
from ferdelance.database.tables import Artifact, Component, Job


@pytest.mark.asyncio
async def test_jobs_list(async_session: AsyncSession):

    c1: Component = Component(
        client_id="C1",
        version="test",
        public_key="1",
        machine_system="1",
        machine_mac_address="1",
        machine_node="1",
        ip_address="1",
        type="CLIENT",
    )
    a1: Artifact = Artifact(artifact_id="A1", path="test-path", status="S1")
    j1: Job = Job(artifact_id="A1", client_id="C1", status="J1")

    async_session.add(c1)
    async_session.add(a1)
    async_session.add(j1)

    await async_session.commit()

    res: pd.DataFrame = await list_jobs()

    assert len(res) == 1

    res: pd.DataFrame = await list_jobs(artifact_id="A1")

    assert len(res) == 1

    res: pd.DataFrame = await list_jobs(client_id="C1")

    assert len(res) == 1

    res: pd.DataFrame = await list_jobs(client_id="C7")

    assert len(res) == 0
