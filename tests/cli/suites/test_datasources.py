from ferdelance.cli.fdl_suites.datasources.functions import describe_datasource, list_datasources
from ferdelance.database.data import TYPE_CLIENT
from ferdelance.database.repositories import DataSourceService, ProjectService
from ferdelance.database.tables import Component
from ferdelance.schemas.datasources import DataSource as DataSourceView
from ferdelance.schemas.metadata import MetaDataSource

from sqlalchemy.ext.asyncio import AsyncSession

import pytest
import pytest


async def populate_test_db(session: AsyncSession):
    c1: Component = Component(
        component_id="C1",
        version="test",
        public_key="1",
        machine_system="1",
        machine_mac_address="1",
        machine_node="1",
        ip_address="1",
        type_name=TYPE_CLIENT,
    )
    c2: Component = Component(
        component_id="C2",
        version="test",
        public_key="2",
        machine_system="2",
        machine_mac_address="2",
        machine_node="2",
        ip_address="2",
        type_name=TYPE_CLIENT,
    )

    session.add(c1)
    session.add(c2)

    await session.commit()

    dss = DataSourceService(session)
    ps = ProjectService(session)

    await ps.create("test")

    ds1 = await dss.create_or_update_datasource(
        c1.component_id,
        MetaDataSource(
            name="DS1",
            datasource_hash="DS1",
            n_records=420,
            n_features=420,
            tokens=[""],
            features=[],
        ),
    )
    ds2 = await dss.create_or_update_datasource(
        c1.component_id,
        MetaDataSource(
            name="DS2",
            datasource_hash="DS2",
            n_records=420,
            n_features=420,
            tokens=[""],
            features=[],
        ),
    )
    ds3 = await dss.create_or_update_datasource(
        c2.component_id,
        MetaDataSource(
            name="DS3",
            datasource_hash="DS3",
            n_records=69,
            n_features=69,
            tokens=[""],
            features=[],
        ),
    )

    return ds1.datasource_id, ds2.datasource_id, ds3.datasource_id


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
