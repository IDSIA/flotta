from ferdelance.cli.fdl_suites.projects.functions import create_project, describe_project, list_projects
from ferdelance.schemas.project import BaseProject as ProjectView
from ferdelance.database.tables import Project as ProjectDB

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

import pytest


@pytest.mark.asyncio
async def test_projects_list(session: AsyncSession):
    p1: ProjectDB = ProjectDB(
        id="P1",
        name="P1",
        token="P1",
    )

    p2: ProjectDB = ProjectDB(
        id="P2",
        name="P2",
        token="P2",
    )

    session.add(p1)
    session.add(p2)

    res: list[ProjectView] = await list_projects()
    assert len(res) == 0

    await session.commit()

    res: list[ProjectView] = await list_projects()
    assert len(res) == 2

    assert res[0].name == "P1"


@pytest.mark.asyncio
async def test_project_create(session: AsyncSession):
    project_token: str = await create_project(name="P1")

    res = await session.execute(select(ProjectDB))
    project_db_list = res.scalars().all()

    assert len(project_db_list) == 1
    assert project_db_list[0].token == project_token


@pytest.mark.asyncio
async def test_project_describe(session: AsyncSession):
    p1: ProjectDB = ProjectDB(
        id="P1",
        name="P1",
        token="P1",
    )

    p2: ProjectDB = ProjectDB(
        id="P2",
        name="P2",
        token="P2",
    )

    session.add(p1)
    session.add(p2)

    await session.commit()

    p1_view: ProjectView | None = await describe_project(token="P1")

    assert p1_view is not None
    assert p1_view.name == "P1"
    assert p1_view.token == "P1"

    p1_view: ProjectView | None = await describe_project(project_id="P1")

    assert p1_view is not None
    assert p1_view.name == "P1"
    assert p1_view.token == "P1"

    res = await describe_project(token="do not exist")
    assert res is None

    with pytest.raises(ValueError) as e:
        res = await describe_project()
        assert "Specify either the project_id or the token of the project" in str(e)

    with pytest.raises(ValueError) as e:
        res = await describe_project(project_id="P1", token="P1")
        assert "Specify either the project_id or the token of the project" in str(e)
