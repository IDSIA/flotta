import pandas as pd
import pytest

from ferdelance.cli.jobs.functions import get_jobs_list
from ferdelance.database import AsyncSession
from ferdelance.database.tables import Artifact, Client, Job


@pytest.mark.asyncio
async def test_jobs_list(async_session: AsyncSession):

    c1: Client = Client(
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

    res: pd.DataFrame = await get_jobs_list()

    assert len(res) == 1

    res: pd.DataFrame = await get_jobs_list(artifact_id="A1")

    assert len(res) == 1

    res: pd.DataFrame = await get_jobs_list(client_id="C1")

    assert len(res) == 1

    res: pd.DataFrame = await get_jobs_list(client_id="C7")

    assert len(res) == 0
