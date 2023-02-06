from typing import Any

from ferdelance.config import conf
from ferdelance.database.services.component import ComponentService
from ferdelance.database.tables import (
    Application,
    DataSource,
    Feature,
    Job,
    Token as TokenDB,
)
from ferdelance.database.schemas import (
    Component,
    Client,
    Event,
    Token,
)
from ferdelance.server.api import api
from ferdelance.shared.actions import Action
from ferdelance.schemas.artifacts import (
    Artifact,
    ArtifactStatus,
    Dataset,
    Metadata,
    Query,
    QueryFeature,
    QueryFilter,
)
from ferdelance.schemas.artifacts.operations import Operations
from ferdelance.shared.exchange import Exchange
from ferdelance.schemas.models import Model
from ferdelance.schemas import (
    ClientUpdate,
    ClientJoinRequest,
    DownloadApp,
    UpdateClientApp,
    UpdateExecute,
    UpdateToken,
    WorkbenchJoinData,
    WorkbenchJoinRequest,
)

from tests.utils import (
    create_client,
    get_metadata,
    send_metadata,
)

from fastapi.testclient import TestClient

from requests import Response
from sqlalchemy import select, update, func
from sqlalchemy.ext.asyncio import AsyncSession

import json
import logging
import os
import pytest
import shutil

LOGGER = logging.getLogger(__name__)


def get_client_update(client: TestClient, exchange: Exchange) -> tuple[int, str, Any]:
    payload = ClientUpdate(action=Action.DO_NOTHING.name)

    response = client.get("/client/update", data=exchange.create_payload(payload.dict()), headers=exchange.headers())

    if response.status_code != 200:
        return response.status_code, "", None

    response_payload = exchange.get_payload(response.content)

    assert "action" in response_payload

    return response.status_code, response_payload["action"], response_payload


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

        cs: ComponentService = ComponentService(session)

        db_client: Component | Client = await cs.get_by_id(client_id)

        assert isinstance(db_client, Client)
        assert db_client.active

        db_token: Token = await cs.get_token_by_component_id(client_id)

        assert db_token is not None
        assert db_token.token == exchange.token
        assert db_token.valid

        db_events: list[Event] = await cs.get_events(client_id)

        assert len(db_events) == 1
        assert db_events[0].event == "creation"


@pytest.mark.asyncio
async def test_client_already_exists(session: AsyncSession, exchange: Exchange):
    """This test will send twice the access information and expect the second time to receive a 403 error."""

    with TestClient(api) as client:
        client_id = create_client(client, exchange)

        cs: ComponentService = ComponentService(session)

        client_db: Client = await cs.get_client_by_id(client_id)

        assert client_db is not None
        assert client_db.machine_system is not None
        assert client_db.machine_mac_address is not None
        assert client_db.machine_node is not None

        data = ClientJoinRequest(
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

        cs: ComponentService = ComponentService(session)

        status_code, action, _ = get_client_update(client, exchange)

        assert status_code == 200
        assert Action[action] == Action.DO_NOTHING

        db_events: list[Event] = await cs.get_events(client_id)
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

        cs: ComponentService = ComponentService(session)

        response_leave = client.post("/client/leave", headers=exchange.headers())

        LOGGER.info(f"response_leave={response_leave}")

        assert response_leave.status_code == 200

        # cannot get other updates
        status_code, _, _ = get_client_update(client, exchange)

        assert status_code == 403

        db_client: Client = await cs.get_client_by_id(client_id)

        assert db_client is not None
        assert db_client.active is False
        assert db_client.left

        db_events: list[Event] = await cs.get_events(client_id)
        events: list[str] = [e.event for e in db_events]

        assert "creation" in events
        assert "left" in events
        assert "update" not in events


@pytest.mark.asyncio
async def test_client_update_token(session: AsyncSession, exchange: Exchange):
    """This will test the failure and update of a token."""
    with TestClient(api) as client:
        client_id = create_client(client, exchange)

        cs: ComponentService = ComponentService(session)

        # expire token
        await session.execute(update(TokenDB).where(TokenDB.component_id == client_id).values(expiration_time=0))
        await session.commit()

        LOGGER.info("expiration_time for token set to 0")

        status_code, action, data = get_client_update(client, exchange)

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

        status_code, _, _ = get_client_update(client, exchange)

        assert status_code == 403

        exchange.set_token(new_token)
        status_code, action, data = get_client_update(client, exchange)

        assert status_code == 200
        assert "action" in data
        assert Action[action] == Action.DO_NOTHING
        assert len(data) == 1


@pytest.mark.asyncio
async def test_client_update_app(session: AsyncSession, exchange: Exchange):
    """This will test the upload of a new (fake) app, and the update process."""
    with TestClient(api) as client:
        client_id = create_client(client, exchange)

        cs: ComponentService = ComponentService(session)

        client_db: Client = await cs.get_client_by_id(client_id)

        assert client_db is not None

        client_version = client_db.version

        assert client_version == "test"

        # create fake app (it's just a file)
        version_app = "test_1.0"
        filename_app = "fake_client_app.json"
        path_fake_app = os.path.join(".", filename_app)

        with open(path_fake_app, "w") as f:
            json.dump({"version": version_app}, f)

        LOGGER.info(f"created file={path_fake_app}")

        # upload fake client app
        upload_response = client.post(
            "/manager/upload/client", files={"file": (filename_app, open(path_fake_app, "rb"))}
        )

        assert upload_response.status_code == 200

        upload_id = upload_response.json()["upload_id"]

        # update metadata
        metadata_response = client.post(
            "/manager/upload/client/metadata",
            json={
                "upload_id": upload_id,
                "version": version_app,
                "name": "Testing_app",
                "active": True,
            },
        )

        assert metadata_response.status_code == 200

        res = await session.execute(select(Application).where(Application.app_id == upload_id))
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
        status_code, action, data = get_client_update(client, exchange)

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

        client_db: Client = await cs.get_client_by_id(client_id)

        assert client_db is not None
        assert client_db.version == version_app

        # delete local fake client app
        if os.path.exists(path_fake_app):
            os.remove(path_fake_app)


@pytest.mark.asyncio
async def test_update_metadata(session: AsyncSession, exchange: Exchange):
    with TestClient(api) as client:
        client_id = create_client(client, exchange)

        assert client_id is not None

        metadata: Metadata = get_metadata()
        upload_response: Response = send_metadata(client, exchange, metadata)

        assert upload_response.status_code == 200

        res = await session.execute(select(DataSource).where(DataSource.component_id == client_id))
        ds_db: DataSource = res.scalar_one()

        assert ds_db.name == metadata.datasources[0].name
        assert ds_db.removed == metadata.datasources[0].removed
        assert ds_db.n_records == metadata.datasources[0].n_records
        assert ds_db.n_features == metadata.datasources[0].n_features

        ds_features = metadata.datasources[0].features

        res = await session.scalars(select(Feature).where(Feature.datasource_id == ds_db.datasource_id))
        ds_fs: list[Feature] = list(res.all())

        assert len(ds_fs) == 2
        assert ds_fs[0].name == ds_features[0].name
        assert ds_fs[1].name == ds_features[1].name


@pytest.mark.asyncio
async def test_client_task_get(session: AsyncSession, exchange: Exchange):
    with TestClient(api) as server:
        client_id = create_client(server, exchange)

        LOGGER.info("setup metadata for client")

        metadata: Metadata = get_metadata()
        upload_response: Response = send_metadata(server, exchange, metadata)

        assert upload_response.status_code == 200

        LOGGER.info("setup artifact")

        res = await session.execute(select(DataSource).where(DataSource.component_id == client_id))
        ds_db: DataSource | None = res.scalar_one_or_none()

        assert ds_db is not None

        res = await session.scalars(
            select(Feature).where(Feature.datasource_id == ds_db.datasource_id, Feature.removed == False)
        )
        fs = list(res.all())

        assert len(fs) == 2

        f1: Feature = fs[0]
        f2: Feature = fs[1]

        qf1 = QueryFeature(
            feature_id=f1.feature_id,
            feature_name=f1.name,
            datasource_id=f1.datasource_id,
            datasource_name=f1.datasource_name,
        )
        qf2 = QueryFeature(
            feature_id=f2.feature_id,
            feature_name=f2.name,
            datasource_id=f2.datasource_id,
            datasource_name=f2.datasource_name,
        )

        artifact = Artifact(
            artifact_id=None,
            dataset=Dataset(
                queries=[
                    Query(
                        datasource_id=ds_db.datasource_id,
                        datasource_name=ds_db.name,
                        features=[qf1, qf2],
                        filters=[QueryFilter(feature=qf1, operation=Operations.NUM_LESS_THAN.name, parameter="1.0")],
                        transformers=[],
                    )
                ],
            ),
            model=Model(name=""),
        )

        LOGGER.info("submit artifact")

        wb_exc = Exchange()
        wb_exc.generate_key()

        wjr = WorkbenchJoinRequest(public_key=wb_exc.transfer_public_key())

        res = server.post("/workbench/connect", data=json.dumps(wjr.dict()))

        res.raise_for_status()

        wjd = WorkbenchJoinData(**wb_exc.get_payload(res.content))

        wb_exc.set_remote_key(wjd.public_key)
        wb_exc.set_token(wjd.token)

        submit_response = server.post(
            "/workbench/artifact/submit",
            data=wb_exc.create_payload(artifact.dict()),
            headers=wb_exc.headers(),
        )

        assert submit_response.status_code == 200

        artifact_status = ArtifactStatus(**wb_exc.get_payload(submit_response.content))

        n = await session.scalar(select(func.count()).select_from(Job))
        assert n == 1

        n = await session.scalar(select(func.count()).select_from(Job).where(Job.component_id == client_id))
        assert n == 1

        res = await session.scalars(select(Job).limit(1))
        job: Job = res.one()

        LOGGER.info("update client")

        status_code, action, data = get_client_update(server, exchange)

        assert status_code == 200
        assert Action[action] == Action.EXECUTE

        update_execute = UpdateExecute(**data)

        assert update_execute.artifact_id == job.artifact_id

        LOGGER.info("get task for client")

        with server.get(
            "/client/task/",
            data=exchange.create_payload(update_execute.dict()),
            headers=exchange.headers(),
            stream=True,
        ) as task_response:
            assert task_response.status_code == 200

            content = exchange.get_payload(task_response.content)

            assert "artifact_id" in content
            assert "model" in content
            assert "dataset" in content

            task = Artifact(**content)

            assert task.artifact_id == job.artifact_id
            assert artifact_status.artifact_id is not None
            assert task.artifact_id == artifact_status.artifact_id
            assert len(task.dataset.queries) == 1
            assert len(task.dataset.queries[0].features) == 2

        # cleanup
        LOGGER.info("cleaning up")
        shutil.rmtree(os.path.join(conf.STORAGE_ARTIFACTS, artifact_status.artifact_id))


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
            "/workbench/client/list",
            headers=exchange.headers(),
        )

        assert res.status_code == 403
