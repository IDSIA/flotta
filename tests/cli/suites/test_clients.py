from ferdelance.cli.fdl_suites.clients.functions import describe_client, list_clients
from ferdelance.const import TYPE_CLIENT
from ferdelance.database import AsyncSession
from ferdelance.database.tables import Component
from ferdelance.schemas.components import Component as ComponentView

import pytest


@pytest.mark.asyncio
async def test_list_clients(session: AsyncSession):
    c1: Component = Component(
        id="C1",
        name="client1",
        version="test",
        public_key="1",
        ip_address="1",
        url="",
        type_name=TYPE_CLIENT,
    )

    c2: Component = Component(
        id="C2",
        name="client2",
        version="test",
        public_key="2",
        ip_address="2",
        url="",
        type_name=TYPE_CLIENT,
    )

    session.add(c1)

    session.add(c2)

    await session.commit()

    res: list[ComponentView] = await list_clients()

    assert len(res) == 2


@pytest.mark.asyncio
async def test_describe_client(session: AsyncSession):
    c1: Component = Component(
        id="C1",
        name="client1",
        version="test",
        public_key="1",
        ip_address="1",
        url="",
        type_name=TYPE_CLIENT,
    )
    session.add(c1)

    await session.commit()

    res: ComponentView | None = await describe_client(client_id=c1.id)

    assert res is not None
    assert isinstance(res, ComponentView)
    assert res.id == "C1"
    assert res.version == "test"

    res = await describe_client(client_id="do not exist")
    assert res is None
