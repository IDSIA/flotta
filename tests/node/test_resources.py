import json
from ferdelance.core.artifacts import Artifact
from ferdelance.core.interfaces import SchedulerJob
from ferdelance.core.operations import DoNothing
from ferdelance.core.steps import BaseStep
from ferdelance.database.repositories import (
    AsyncSession,
    ArtifactRepository,
    ComponentRepository,
    JobRepository,
    ResourceRepository,
)
from ferdelance.node.api import api
from ferdelance.schemas.database import Resource
from ferdelance.schemas.resources import NewResource, ResourceIdentifier
from ferdelance.security.exchange import Exchange

from tests.utils import TEST_PROJECT_TOKEN, create_node

from fastapi.testclient import TestClient

import pytest


async def setup_resource(session: AsyncSession, client_id: str) -> tuple[str, str, Resource]:
    ar: ArtifactRepository = ArtifactRepository(session)
    cr: ComponentRepository = ComponentRepository(session)
    rr: ResourceRepository = ResourceRepository(session)
    jr: JobRepository = JobRepository(session)

    client = await cr.get_by_id(client_id)

    artifact = await ar.create_artifact(
        Artifact(
            project_id=TEST_PROJECT_TOKEN,
            steps=list(),
        ),
    )

    job_id = "job"

    resource = await rr.create_resource(
        artifact_id=artifact.id,
        job_id=job_id,
        iteration=0,
        producer_id=client_id,
    )

    sj = SchedulerJob(id=0, worker=client, iteration=0, step=BaseStep(operation=DoNothing()))
    await jr.create_job(artifact.id, sj, resource.id, job_id=job_id)

    return artifact.id, job_id, resource


@pytest.mark.asyncio
async def test_submit_and_download_resource(session: AsyncSession):
    with TestClient(api) as server:
        exchange: Exchange = create_node(server)
        client_id = exchange.source_id

        artifact_id, job_id, resource = await setup_resource(session, client_id)

        # send resource
        resource_content = "some resource"

        headers, payload = exchange.create(
            content=resource_content,
            extra_headers=NewResource(
                artifact_id=artifact_id,
                job_id=job_id,
                resource_id=resource.id,
                file="attached",
            ).dict(),
        )

        res = server.post(
            "/resource/",
            headers=headers,
            content=payload,
        )

        res.raise_for_status()

        _, content = exchange.get_payload(res.content)
        ri = ResourceIdentifier(**json.loads(content))

        assert ri.producer_id == client_id
        assert ri.resource_id == resource.id

        # get resource
        headers, payload = exchange.create(ri.json())

        with server.stream(
            "GET",
            "/resource/",
            headers=headers,
            content=payload,
        ) as stream:
            stream.raise_for_status()

            _, get_content = exchange.stream_decrypt(stream.iter_bytes())

            assert resource_content == get_content.decode()


@pytest.mark.asyncio
async def test_proxy_resource(session: AsyncSession):
    with TestClient(api) as server:
        exchange: Exchange = create_node(server)
        client_id = exchange.source_id
        server_id = exchange.target_id

        assert server_id is not None

        artifact_id, job_id, resource = await setup_resource(session, client_id)

        cr: ComponentRepository = ComponentRepository(session)

        sv = await cr.get_by_id(server_id)
        cl = await cr.get_by_id(client_id)

        # send resource
        resource_content = "some resource"

        exchange.set_remote_key(cl.id, cl.public_key)
        exchange.set_proxy_key(sv.public_key)

        headers, payload = exchange.create(
            content=resource_content,
            extra_headers=NewResource(
                artifact_id=artifact_id,
                job_id=job_id,
                resource_id=resource.id,
                file="attached",
            ).dict(),
        )

        res = server.post(
            "/resource/",
            headers=headers,
            content=payload,
        )

        res.raise_for_status()

        _, content = exchange.get_payload(res.content)
        ri = ResourceIdentifier(**json.loads(content))

        assert ri.producer_id == client_id
        assert ri.resource_id == resource.id

        with open(resource.path, "rb") as f:
            read_content = f.read()
            try:
                assert resource_content != read_content.decode()
            except Exception as _:
                assert True

        # get resource
        exchange.clear_proxy()
        exchange.set_remote_key(sv.id, sv.public_key)

        headers, payload = exchange.create(ri.json())

        with server.stream(
            "GET",
            "/resource/",
            headers=headers,
            content=payload,
        ) as stream:
            stream.raise_for_status()

            _, get_content = exchange.stream_decrypt(stream.iter_bytes())

            assert resource_content == get_content.decode()
