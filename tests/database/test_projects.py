from ferdelance.database.repositories import ProjectRepository
from ferdelance.server.api import api
from ferdelance.shared.exchange import Exchange

from tests.utils import create_project, create_client, get_metadata, send_metadata

from fastapi.testclient import TestClient
from sqlalchemy.ext.asyncio import AsyncSession

import pytest
import json


@pytest.mark.asyncio
async def test_load_project(session: AsyncSession, exchange: Exchange):
    with TestClient(api) as client:
        p_token: str = "123456789"
        metadata = get_metadata(project_token=p_token)

        await create_project(session, p_token)
        create_client(client, exchange)
        send_metadata(client, exchange, metadata)

        pr: ProjectRepository = ProjectRepository(session)

        list_project = await pr.list_projects()

        assert len(list_project) == 2

        p = await pr.get_by_token(p_token)

        assert p.n_datasources == 1
        assert p.data.n_features == 2
        assert p.data.n_datasources == 1
        assert p.data.n_clients == 1

        print(json.dumps(p.dict(), indent=True, default=str))
