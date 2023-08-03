from typing import AsyncGenerator

from ferdelance.config import get_config
from ferdelance.database import Base, DataBase
from ferdelance.database.data import COMPONENT_TYPES
from ferdelance.database.tables import ComponentType
from ferdelance.shared.exchange import Exchange

from sqlalchemy.ext.asyncio import AsyncSession

import os
import pytest
import pytest_asyncio
import shutil


db_file = "./tests/test_sqlite.db"
db_path = os.path.join("./", db_file)

conf = get_config()

conf.database.memory = False
conf.database.dialect = "sqlite"
conf.database.host = db_file

conf.server.main_password = "7386ee647d14852db417a0eacb46c0499909aee90671395cb5e7a2f861f68ca1"
conf.private_key_location = str(os.path.join("tests", "private_key.pem"))


def create_dirs() -> None:
    os.makedirs(conf.storage_datasources_dir(), exist_ok=True)
    os.makedirs(conf.storage_artifact_dir(), exist_ok=True)
    os.makedirs(conf.storage_clients_dir(), exist_ok=True)
    os.makedirs(conf.storage_results_dir(), exist_ok=True)

    if os.path.exists(db_path):
        os.remove(db_path)


def delete_dirs() -> None:
    shutil.rmtree(conf.storage_datasources_dir())
    shutil.rmtree(conf.storage_artifact_dir())
    shutil.rmtree(conf.storage_clients_dir())
    shutil.rmtree(conf.storage_results_dir())

    os.remove(db_path)


@pytest_asyncio.fixture()
async def session() -> AsyncGenerator[AsyncSession, None]:
    create_dirs()

    inst = DataBase()

    async with inst.engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
        try:
            async with inst.session() as session:
                for t in COMPONENT_TYPES:
                    session.add(ComponentType(type=t))
                await session.commit()
                yield session
        finally:
            await conn.run_sync(Base.metadata.drop_all)
            delete_dirs()


@pytest.fixture()
def exchange() -> Exchange:
    exc = Exchange()
    exc.generate_key()

    return exc
