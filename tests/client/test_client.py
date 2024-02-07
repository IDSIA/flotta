from ferdelance.database.repositories import ComponentRepository
from ferdelance.database.tables import DataSource as DataSourceDB
from ferdelance.logging import get_logger
from ferdelance.node.api import api
from ferdelance.schemas.components import Component
from ferdelance.schemas.metadata import Metadata
from ferdelance.security.exchange import Exchange
from ferdelance.shared.actions import Action

from tests.utils import (
    create_node,
    get_metadata,
    send_metadata,
    client_update,
)

from fastapi.testclient import TestClient

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from httpx import HTTPStatusError

import os
import pytest

LOGGER = get_logger(__name__)


@pytest.mark.asyncio
async def test_client_read_home():
    """Generic test to check if the home works."""
    with TestClient(api) as client:
        response = client.get("/")

        assert response.status_code == 200
        assert response.content.decode("utf8") == '"Hi! 😀"'


@pytest.mark.asyncio
async def test_client_connect_successful(session: AsyncSession):
    """Simulates the arrival of a new client. The client will connect with a set of hardcoded values:
    - operative system
    - mac address
    - node identification number
    - its public key
    - the version of the software in use

    Then the server will answer with:
    - an encrypted token
    - an encrypted client id
    - a public key in str format
    """

    with TestClient(api) as client:
        exchange: Exchange = create_node(client)
        client_id = exchange.source_id

        cr: ComponentRepository = ComponentRepository(session)

        db_client: Component = await cr.get_client_by_id(client_id)

        assert db_client.active


@pytest.mark.asyncio
async def test_client_already_exists(session: AsyncSession):
    """This test will send twice the access information and expect the second time to receive a 403 error."""

    with TestClient(api) as client:
        exchange: Exchange = create_node(client)
        client_id = exchange.source_id

        cr: ComponentRepository = ComponentRepository(session)

        await cr.get_client_by_id(client_id)

        try:
            create_node(client, client_id=client_id)
            assert False

        except HTTPStatusError as e:
            assert "403" in str(e)


@pytest.mark.asyncio
async def test_client_update(session: AsyncSession):
    """This will test the endpoint for updates."""

    with TestClient(api) as client:
        exchange: Exchange = create_node(client)

        status_code, action, _ = client_update(client, exchange)

        assert status_code == 200
        assert Action[action] == Action.DO_NOTHING


@pytest.mark.asyncio
async def test_client_leave(session: AsyncSession):
    """This will test the endpoint for leave a client."""
    with TestClient(api) as client:
        exchange: Exchange = create_node(client)
        client_id = exchange.source_id

        cr: ComponentRepository = ComponentRepository(session)

        headers, payload = exchange.create()

        response_leave = client.post(
            "/node/leave",
            headers=headers,
            content=payload,
        )

        LOGGER.info(f"response_leave={response_leave}")

        assert response_leave.status_code == 200

        # cannot get other updates
        status_code, _, _ = client_update(client, exchange)

        assert status_code == 403

        db_client: Component = await cr.get_client_by_id(client_id)

        assert db_client is not None
        assert db_client.active is False
        assert db_client.left


@pytest.mark.asyncio
async def test_update_metadata(session: AsyncSession):
    with TestClient(api) as client:
        exchange: Exchange = create_node(client)
        client_id = exchange.source_id

        assert client_id is not None

        metadata: Metadata = get_metadata()
        send_metadata(client, exchange, metadata)

        res = await session.execute(select(DataSourceDB).where(DataSourceDB.component_id == client_id))
        ds_db: DataSourceDB = res.scalar_one()

        assert ds_db.name == metadata.datasources[0].name
        assert ds_db.removed == metadata.datasources[0].removed
        assert ds_db.n_records == metadata.datasources[0].n_records
        assert ds_db.n_features == metadata.datasources[0].n_features

        assert os.path.exists(ds_db.path)


@pytest.mark.asyncio
async def test_client_access(session: AsyncSession):
    with TestClient(api) as client:
        exchange: Exchange = create_node(client)

        headers, payload = exchange.create('{"action":""}')

        res = client.request(
            method="GET",
            url="/client/update",
            headers=headers,
            content=payload,
        )

        assert res.status_code == 200

        # TODO: endpoints not protected
        """
        res = client.request(
            "GET",
            "/nodes/",
            headers=headers,
            content=payload,
        )

        assert res.status_code == 200

        res = client.request(
            "GET",
            "/task/",
            headers=headers,
            content=payload,
        )

        assert res.status_code == 200
        """

        res = client.request(
            "GET",
            "/workbench/clients",
            headers=headers,
            content=payload,
        )

        assert res.status_code == 403
