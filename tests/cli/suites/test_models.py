from ferdelance.cli.fdl_suites.resources.functions import describe_resource, list_resource
from ferdelance.const import TYPE_CLIENT
from ferdelance.schemas.database import Resource
from ferdelance.database.tables import Artifact, Component, Resource as ResourceDB

from sqlalchemy.ext.asyncio import AsyncSession

import pytest


@pytest.mark.asyncio
async def test_resources_list(session: AsyncSession):
    session.add(
        Artifact(
            id="aid1",
            path=".",
            status="",
        )
    )

    session.add(
        Component(
            id="cid1",
            name="client1",
            version="test",
            public_key="1",
            ip_address="1",
            url="",
            type_name=TYPE_CLIENT,
        )
    )

    session.add(
        ResourceDB(
            id="mid1",
            job_id="job-1",
            path=".",
            artifact_id="aid1",
            component_id="cid1",
        )
    )

    res: list[Resource] = await list_resource()

    assert len(res) == 0

    await session.commit()

    res: list[Resource] = await list_resource()

    assert len(res) == 1


@pytest.mark.asyncio
async def test_describe_client(session: AsyncSession):
    session.add(
        Artifact(
            id="aid1",
            path=".",
            status="",
        )
    )

    session.add(
        Component(
            id="cid1",
            name="client1",
            version="test",
            public_key="1",
            ip_address="1",
            url="",
            type_name=TYPE_CLIENT,
        )
    )

    session.add(
        ResourceDB(
            id="mid1",
            job_id="job-1",
            path=".",
            artifact_id="aid1",
            component_id="cid1",
            creation_time=None,
        )
    )

    await session.commit()

    res: Resource | None = await describe_resource(resource_id="mid1")

    assert res is not None
    assert res.id == "mid1"
    assert res.path == "."

    res = await describe_resource(resource_id="do not exist")
    assert res is None
