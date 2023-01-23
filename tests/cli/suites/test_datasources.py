from typing import List

import pytest
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm.exc import NoResultFound

from ferdelance.cli.suites.datasources.functions import describe_datasource, list_datasources
from ferdelance.database.schemas import DataSource as DataSouceView
from ferdelance.database.tables import Component, DataSource


async def populate_test_db(async_session: AsyncSession):
    c1: Component = Component(
        component_id="C1",
        version="test",
        public_key="1",
        machine_system="1",
        machine_mac_address="1",
        machine_node="1",
        ip_address="1",
        type="CLIENT",
    )

    c2: Component = Component(
        component_id="C2",
        version="test",
        public_key="2",
        machine_system="2",
        machine_mac_address="2",
        machine_node="2",
        ip_address="2",
        type="CLIENT",
    )

    ds1: DataSource = DataSource(
        datasource_id="DS1", name="DS1", n_records=420, n_features=420, component_id=c1.component_id
    )
    ds2: DataSource = DataSource(
        datasource_id="DS2", name="DS2", n_records=420, n_features=420, component_id=c1.component_id
    )
    ds3: DataSource = DataSource(
        datasource_id="DS3", name="DS3", n_records=69, n_features=69, component_id=c2.component_id
    )

    async_session.add(c1)
    async_session.add(c2)
    async_session.add(ds1)
    async_session.add(ds2)
    async_session.add(ds3)

    await async_session.commit()


@pytest.mark.asyncio
async def test_datasources_ls(async_session: AsyncSession):

    res: List[DataSouceView] = await list_datasources()

    assert len(res) == 0

    await populate_test_db(async_session)

    res: List[DataSouceView] = await list_datasources()

    assert len(res) == 3

    res: List[DataSouceView] = await list_datasources(component_id="C1")

    assert len(res) == 2


@pytest.mark.asyncio
async def test_artifacts_description(async_session: AsyncSession):

    await populate_test_db(async_session)

    res: DataSource = await describe_datasource(datasource_id="DS1")

    assert res.name == "DS1"

    with pytest.raises(ValueError) as e:
        res = await describe_datasource(datasource_id=None)
    assert "Provide a DataSource ID" in str(e)

    with pytest.raises(NoResultFound) as e:
        res = await describe_datasource(datasource_id="do_not_exist")
    assert "No row was found when one was required" in str(e)
