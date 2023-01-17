import pandas as pd
import pytest

from ferdelance.cli.clients.functions import describe_client, list_clients
from ferdelance.database import AsyncSession
from ferdelance.database.tables import Client


@pytest.mark.asyncio
async def test_list_clients(async_session: AsyncSession):

    async_session.add(
        Client(
            client_id="C1",
            version="test",
            public_key="1",
            machine_system="1",
            machine_mac_address="1",
            machine_node="1",
            ip_address="1",
            type="CLIENT",
        )
    )

    async_session.add(
        Client(
            client_id="C2",
            version="test",
            public_key="2",
            machine_system="2",
            machine_mac_address="2",
            machine_node="2",
            ip_address="2",
            type="CLIENT",
        )
    )

    await async_session.commit()

    res: pd.DataFrame = await list_clients()

    assert len(res) == 2


@pytest.mark.asyncio
async def test_describe_client(async_session: AsyncSession):
    async_session.add(
        Client(
            client_id="C1",
            version="test",
            public_key="1",
            machine_system="1",
            machine_mac_address="1",
            machine_node="1",
            ip_address="1",
            type="CLIENT",
        )
    )

    await async_session.commit()

    res = await describe_client(client_id="C1")

    assert res.client_id == "C1"
    assert res.version == "test"
