from ferdelance.workbench.interface import Artifact

from tests.utils import TEST_PROJECT_TOKEN, get_metadata
from tests.serverless import ServerlessExecution, ServerlessWorker

from sqlalchemy.ext.asyncio import AsyncSession

import pytest

from hashlib import sha256
from uuid import uuid5, NAMESPACE_URL


@pytest.mark.asyncio
async def test_count(session: AsyncSession):
    node0 = ServerlessExecution(session)
    await node0.setup()
    await node0.create_project(TEST_PROJECT_TOKEN)

    ds_id_1: str = str(uuid5(NAMESPACE_URL, "ds_1"))
    ds_id_2: str = str(uuid5(NAMESPACE_URL, "ds_2"))
    ds_id_3: str = str(uuid5(NAMESPACE_URL, "ds_3"))

    ds_hs_1: str = sha256(ds_id_1.encode()).hexdigest()
    ds_hs_2: str = sha256(ds_id_2.encode()).hexdigest()
    ds_hs_3: str = sha256(ds_id_3.encode()).hexdigest()

    node1: ServerlessWorker = await node0.add_worker(1, get_metadata(TEST_PROJECT_TOKEN, ds_id_1, ds_hs_1))
    node2: ServerlessWorker = await node0.add_worker(2, get_metadata(TEST_PROJECT_TOKEN, ds_id_2, ds_hs_2))
    node3: ServerlessWorker = await node0.add_worker(2, get_metadata(TEST_PROJECT_TOKEN, ds_id_3, ds_hs_3))

    project = await node0.get_project(TEST_PROJECT_TOKEN)

    q = project.extract().count()

    art_count = Artifact(
        project_id=project.id,
        steps=[],
    )

    await node0.submit(art_count)
