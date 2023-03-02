from typing import AsyncGenerator

from ferdelance.config import conf
from ferdelance.database import Base, DataBase
from ferdelance.database.data import COMPONENT_TYPES
from ferdelance.database.tables import ComponentType
from ferdelance.shared.exchange import Exchange

from sqlalchemy.ext.asyncio import AsyncSession

import logging
import os
import pytest
import pytest_asyncio
import shutil

LOGGER = logging.getLogger(__name__)

db_file = "./tests/test_sqlite.db"
db_path = os.path.join("./", db_file)

conf.DB_MEMORY = False
conf.DB_DIALECT = "sqlite"
conf.DB_HOST = db_file

conf.SERVER_MAIN_PASSWORD = "7386ee647d14852db417a0eacb46c0499909aee90671395cb5e7a2f861f68ca1"

# this is for client
os.environ["PATH_PRIVATE_KEY"] = os.environ.get("PATH_PRIVATE_KEY", str(os.path.join("tests", "private_key.pem")))


def create_dirs() -> None:
    os.makedirs(conf.STORAGE_DATASOURCES, exist_ok=True)
    os.makedirs(conf.STORAGE_ARTIFACTS, exist_ok=True)
    os.makedirs(conf.STORAGE_CLIENTS, exist_ok=True)
    os.makedirs(conf.STORAGE_RESULTS, exist_ok=True)

    if os.path.exists(db_path):
        os.remove(db_path)


def delete_dirs() -> None:
    shutil.rmtree(conf.STORAGE_DATASOURCES)
    shutil.rmtree(conf.STORAGE_ARTIFACTS)
    shutil.rmtree(conf.STORAGE_CLIENTS)
    shutil.rmtree(conf.STORAGE_RESULTS)

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
