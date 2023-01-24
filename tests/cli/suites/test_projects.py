from typing import List

import pytest
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy import select

from ferdelance.cli.fdl_suites.projects.functions import create_project, describe_project, list_projects
from ferdelance.database.schemas import Project as ProjectView
from ferdelance.database.tables import Project as ProjectDB


@pytest.mark.asyncio
async def test_projects_list(async_session: AsyncSession):
    p1: ProjectDB = ProjectDB(
        project_id="P1",
        name="P1",
        token="P1",
    )

    p2: ProjectDB = ProjectDB(
        project_id="P2",
        name="P2",
        token="P2",
    )

    async_session.add(p1)
    async_session.add(p2)

    res: List[ProjectView] = await list_projects()
    assert len(res) == 0

    await async_session.commit()

    res: List[ProjectView] = await list_projects()
    assert len(res) == 2

    assert res[0].name == "P1"


@pytest.mark.asyncio
async def test_project_create(async_session: AsyncSession):

    project_token: str = await create_project(name="P1")

    res = await async_session.execute(select(ProjectDB))
    project_db_list = res.scalars().all()

    assert len(project_db_list) == 1
    assert project_db_list[0].token == project_token


@pytest.mark.asyncio
async def test_project_describe(async_session: AsyncSession):
    p1: ProjectDB = ProjectDB(
        project_id="P1",
        name="P1",
        token="P1",
    )

    p2: ProjectDB = ProjectDB(
        project_id="P2",
        name="P2",
        token="P2",
    )

    async_session.add(p1)
    async_session.add(p2)

    await async_session.commit()

    p1_view: ProjectView = await describe_project(token="P1")

    assert p1_view.name == "P1"
    assert p1_view.token == "P1"

    p1_view: ProjectView = await describe_project(project_id="P1")

    assert p1_view.name == "P1"
    assert p1_view.token == "P1"

    with pytest.raises(NoResultFound) as e:
        res = await describe_project(token="do not exist")
        assert "No row was found when one was required" in str(e)

    with pytest.raises(ValueError) as e:
        res = await describe_project()
        assert "Specify either the project_id or the token of the project" in str(e)

    with pytest.raises(ValueError) as e:
        res = await describe_project(project_id="P1", token="P1")
        assert "Specify either the project_id or the token of the project" in str(e)
