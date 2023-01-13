import pandas as pd
import pytest

from ferdelance.database import AsyncSession
from ferdelance.database.tables import Client
from ferdelance.server.cli.clients.functions import list_clients


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

    assert False
