from flotta.cli.fdl_suites.datasources.functions import describe_datasource, list_datasources
from flotta.const import TYPE_CLIENT
from flotta.database.repositories import DataSourceRepository, ProjectRepository
from flotta.database.tables import Component
from flotta.schemas.datasources import DataSource as DataSourceView
from flotta.schemas.metadata import MetaDataSource

from sqlalchemy.ext.asyncio import AsyncSession

import pytest


async def populate_test_db(session: AsyncSession):
    c1: Component = Component(
        id="C1",
        name="client1",
        version="test",
        public_key="1",
        ip_address="1",
        url="",
        type_name=TYPE_CLIENT,
    )
    c2: Component = Component(
        id="C2",
        name="client2",
        version="test",
        public_key="2",
        ip_address="2",
        url="",
        type_name=TYPE_CLIENT,
    )

    session.add(c1)
    session.add(c2)

    await session.commit()

    dsr: DataSourceRepository = DataSourceRepository(session)
    pr: ProjectRepository = ProjectRepository(session)

    await pr.create_project("test")

    ds1 = await dsr.create_or_update_datasource(
        c1.id,
        MetaDataSource(
            name="DS1",
            hash="DS1",
            n_records=420,
            n_features=420,
            tokens=[""],
            features=[],
        ),
    )
    ds2 = await dsr.create_or_update_datasource(
        c1.id,
        MetaDataSource(
            name="DS2",
            hash="DS2",
            n_records=420,
            n_features=420,
            tokens=[""],
            features=[],
        ),
    )
    ds3 = await dsr.create_or_update_datasource(
        c2.id,
        MetaDataSource(
            name="DS3",
            hash="DS3",
            n_records=69,
            n_features=69,
            tokens=[""],
            features=[],
        ),
    )

    return ds1.id, ds2.id, ds3.id


@pytest.mark.asyncio
async def test_datasources_ls(session: AsyncSession):
    res: list[DataSourceView] = await list_datasources()

    assert len(res) == 0

    await populate_test_db(session)

    res: list[DataSourceView] = await list_datasources()

    assert len(res) == 3

    res: list[DataSourceView] = await list_datasources(client_id="C1")

    assert len(res) == 2


@pytest.mark.asyncio
async def test_artifacts_description(session: AsyncSession):
    ds1, _, _ = await populate_test_db(session)

    res: DataSourceView | None = await describe_datasource(datasource_id=ds1)

    assert res is not None
    assert res.name == "DS1"

    with pytest.raises(ValueError) as e:
        res: DataSourceView | None = await describe_datasource(datasource_id=None)
    assert "Provide a DataSource ID" in str(e)

    res: DataSourceView | None = await describe_datasource(datasource_id="do_not_exist")
    assert res is None
