import pandas as pd
import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from ferdelance.database import Base, DataBase
from ferdelance.database.tables import Client
from ferdelance.server.cli.clients.functions import describe_client, list_clients

from ...utils import setup_test_database


@pytest.mark.asyncio
async def test_list_clients():
    engine = setup_test_database()
    inst = DataBase()

    async with inst.engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    async with AsyncSession(inst.engine) as session:
        session.add(
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

        session.add(
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

        await session.commit()

        res: pd.DataFrame = list_clients()

        assert len(res) == 2

    assert False
