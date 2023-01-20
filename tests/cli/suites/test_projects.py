from typing import List

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from ferdelance.cli.suites.projects.functions import create_project, list_projects


@pytest.mark.asyncio
async def test_models_list(async_session: AsyncSession):
    result: List = await list_projects()
    assert isinstance(result, list)


@pytest.mark.asyncio
async def test_models_create(async_session: AsyncSession):
    project_token: str = await create_project(name="Test1")
    assert isinstance(project_token, str)
