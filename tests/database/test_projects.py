from tests.utils import create_project

from sqlalchemy.ext.asyncio import AsyncSession

import pytest
import json


@pytest.mark.asyncio
async def test_load_project(session: AsyncSession):

    p_token: str = "123456789"
    ds_hash: str = "abcdefghijklmnopqrstuvwxyz"

    ps, _ = await create_project(session, p_token, ds_hash)

    list_project = await ps.get_project_list()

    assert len(list_project) == 1

    p = await ps.get_by_token(p_token)

    assert p.n_datasources == 1
    assert p.data.n_features == 2
    assert p.data.n_datasources == 1
    assert p.data.n_clients == 1

    print(json.dumps(p.dict(), indent=True, default=str))
