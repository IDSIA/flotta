from ferdelance.config import conf
from ferdelance.database.repositories import ComponentRepository
from ferdelance.database.tables import (
    Application,
    DataSource as DataSourceDB,
    Token as TokenDB,
)
from ferdelance.schemas.components import (
    Component,
    Client,
    Event,
    Token,
)
from ferdelance.schemas.metadata import Metadata
from ferdelance.schemas.client import ClientJoinRequest
from ferdelance.schemas.updates import (
    DownloadApp,
    UpdateClientApp,
    UpdateToken,
)
from ferdelance.server.api import api
from ferdelance.shared.actions import Action
from ferdelance.shared.exchange import Exchange

from tests.utils import (
    create_client,
    get_metadata,
    send_metadata,
    client_update,
)

from fastapi.testclient import TestClient

from requests import Response
from sqlalchemy import select, update, func
from sqlalchemy.ext.asyncio import AsyncSession

import hashlib
import json
import logging
import os
import pytest
import uuid

LOGGER = logging.getLogger(__name__)


@pytest.mark.asyncio
async def test_client_read_home(exchange: Exchange):
    """Generic test to check if the home works."""
    with TestClient(api) as client:
        create_client(client, exchange)

        response = client.get("/")

        assert response.status_code == 200
        assert response.content.decode("utf8") == '"Hi! 😀"'


@pytest.mark.asyncio
async def test_client_connect_successful(session: AsyncSession, exchange: Exchange):
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
        client_id = create_client(client, exchange)

        cr: ComponentRepository = ComponentRepository(session)

        db_client: Client = await cr.get_client_by_id(client_id)

        assert isinstance(db_client, Client)
        assert db_client.active

        db_token: Token = await cr.get_token_by_component_id(client_id)

        assert db_token is not None
        assert db_token.token == exchange.token
        assert db_token.valid

        db_events: list[Event] = await cr.list_events(client_id)

        assert len(db_events) == 1
        assert db_events[0].event == "creation"


@pytest.mark.asyncio
async def test_client_already_exists(session: AsyncSession, exchange: Exchange):
    """This test will send twice the access information and expect the second time to receive a 403 error."""

    with TestClient(api) as client:
        client_id = create_client(client, exchange)

        cr: ComponentRepository = ComponentRepository(session)

        client_db: Client = await cr.get_client_by_id(client_id)

        assert client_db is not None
        assert client_db.machine_system is not None
        assert client_db.machine_mac_address is not None
        assert client_db.machine_node is not None

        data = ClientJoinRequest(
            name="testing_client",
            system=client_db.machine_system,
            mac_address=client_db.machine_mac_address,
            node=client_db.machine_node,
            public_key=exchange.transfer_public_key(),
            version="test",
        )

        response = client.post(
            "/client/join",
            json=data.dict(),
        )

        assert response.status_code == 403
        assert response.json()["detail"] == "Invalid client data"


@pytest.mark.asyncio
async def test_client_update(session: AsyncSession, exchange: Exchange):
    """This will test the endpoint for updates."""

    with TestClient(api) as client:
        client_id = create_client(client, exchange)

        cr: ComponentRepository = ComponentRepository(session)

        status_code, action, _ = client_update(client, exchange)

        assert status_code == 200
        assert Action[action] == Action.DO_NOTHING

        db_events: list[Event] = await cr.list_events(client_id)
        events: list[str] = [e.event for e in db_events]

        assert len(events) == 3
        assert "creation" in events
        assert "update" in events
        assert f"action:{Action.DO_NOTHING.name}" in events


@pytest.mark.asyncio
async def test_client_leave(session: AsyncSession, exchange: Exchange):
    """This will test the endpoint for leave a client."""
    with TestClient(api) as client:
        client_id = create_client(client, exchange)

        cr: ComponentRepository = ComponentRepository(session)

        response_leave = client.post("/client/leave", headers=exchange.headers())

        LOGGER.info(f"response_leave={response_leave}")

        assert response_leave.status_code == 200

        # cannot get other updates
        status_code, _, _ = client_update(client, exchange)

        assert status_code == 403

        db_client: Client = await cr.get_client_by_id(client_id)

        assert db_client is not None
        assert db_client.active is False
        assert db_client.left

        db_events: list[Event] = await cr.list_events(client_id)
        events: list[str] = [e.event for e in db_events]

        assert "creation" in events
        assert "left" in events
        assert "update" not in events


@pytest.mark.asyncio
async def test_client_update_token(session: AsyncSession, exchange: Exchange):
    """This will test the failure and update of a token."""
    with TestClient(api) as client:
        client_id = create_client(client, exchange)

        # expire token
        await session.execute(update(TokenDB).where(TokenDB.component_id == client_id).values(expiration_time=0))
        await session.commit()

        LOGGER.info("expiration_time for token set to 0")

        status_code, action, data = client_update(client, exchange)

        assert "action" in data
        assert Action[action] == Action.UPDATE_TOKEN

        update_token = UpdateToken(**data)

        new_token = update_token.token

        assert status_code == 200
        assert Action[action] == Action.UPDATE_TOKEN

        # extend expire token
        await session.execute(update(TokenDB).where(TokenDB.component_id == client_id).values(expiration_time=86400))
        await session.commit()

        LOGGER.info("expiration_time for token set to 24h")

        status_code, _, _ = client_update(client, exchange)

        assert status_code == 403

        exchange.set_token(new_token)
        status_code, action, data = client_update(client, exchange)

        assert status_code == 200
        assert "action" in data
        assert Action[action] == Action.DO_NOTHING
        assert len(data) == 1


@pytest.mark.asyncio
async def test_client_update_app(session: AsyncSession, exchange: Exchange):
    """This will test the upload of a new (fake) app, and the update process."""
    with TestClient(api) as client:
        client_id = create_client(client, exchange)

        cr: ComponentRepository = ComponentRepository(session)

        client_db: Client = await cr.get_client_by_id(client_id)

        assert client_db is not None

        client_version = client_db.version

        assert client_version == "test"

        # create fake app (it's just a file)
        os.makedirs(conf.STORAGE_CLIENTS, exist_ok=True)

        version_app = "test_1.0"
        filename = "fake_client_app.json"
        path = os.path.join(conf.STORAGE_CLIENTS, filename)

        checksum = hashlib.sha256()

        with open(path, "w") as f:
            content = json.dumps({"version": version_app})
            checksum.update(content.encode("utf8"))
            f.write(content)

        LOGGER.info(f"created file={path}")

        # add fake client app
        app_id = str(uuid.uuid4())
        client_app = Application(
            app_id=app_id,
            path=path,
            name=filename,
            version=version_app,
            checksum=checksum.hexdigest(),
            active=True,
        )
        session.add(client_app)
        await session.commit()

        res = await session.execute(select(Application).where(Application.app_id == app_id))
        client_app: Application | None = res.scalar_one_or_none()

        assert client_app is not None
        assert client_app.version == version_app
        assert client_app.active

        res = await session.execute(select(func.count()).select_from(Application).where(Application.active))
        n_apps: int = res.scalar_one()
        assert n_apps == 1

        res = await session.execute(
            select(Application).where(Application.active).order_by(Application.creation_time.desc()).limit(1)
        )
        newest_version: Application | None = res.scalar_one_or_none()

        assert newest_version is not None
        assert newest_version.version == version_app

        # update request
        status_code, action, data = client_update(client, exchange)

        assert status_code == 200
        assert Action[action] == Action.UPDATE_CLIENT

        update_client_app = UpdateClientApp(**data)

        assert update_client_app.version == version_app

        download_app = DownloadApp(name=update_client_app.name, version=update_client_app.version)

        assert exchange.remote_key is not None

        # download new client
        with client.get(
            "/client/download/application",
            data=exchange.create_payload(download_app.dict()),
            headers=exchange.headers(),
            stream=True,
        ) as stream:
            assert stream.status_code == 200

            content, checksum = exchange.stream_response(stream)

            assert update_client_app.checksum == checksum
            assert json.loads(content) == {"version": version_app}

        client_db: Client = await cr.get_client_by_id(client_id)

        assert client_db is not None
        assert client_db.version == version_app

        # delete local fake client app
        if os.path.exists(path):
            os.remove(path)


@pytest.mark.asyncio
async def test_update_metadata(session: AsyncSession, exchange: Exchange):
    with TestClient(api) as client:
        client_id = create_client(client, exchange)

        assert client_id is not None

        metadata: Metadata = get_metadata()
        upload_response: Response = send_metadata(client, exchange, metadata)

        assert upload_response.status_code == 200

        res = await session.execute(select(DataSourceDB).where(DataSourceDB.component_id == client_id))
        ds_db: DataSourceDB = res.scalar_one()

        assert ds_db.name == metadata.datasources[0].name
        assert ds_db.removed == metadata.datasources[0].removed
        assert ds_db.n_records == metadata.datasources[0].n_records
        assert ds_db.n_features == metadata.datasources[0].n_features

        assert os.path.exists(ds_db.path)


@pytest.mark.asyncio
async def test_client_access(session: AsyncSession, exchange: Exchange):
    with TestClient(api) as client:
        create_client(client, exchange)

        res = client.get(
            "/client/update",
            data=exchange.create_payload({}),
            headers=exchange.headers(),
        )

        assert res.status_code == 200

        res = client.get(
            "/worker/artifact/none",
            headers=exchange.headers(),
        )

        assert res.status_code == 403

        res = client.get(
            "/workbench/clients",
            headers=exchange.headers(),
        )

        assert res.status_code == 403
