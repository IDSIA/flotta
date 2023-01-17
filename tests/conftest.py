from typing import AsyncGenerator, Generator

from ferdelance.config import conf
from ferdelance.database import Base, DataBase
from ferdelance.shared.exchange import Exchange

from sqlalchemy import create_engine
from sqlalchemy.engine import Connection
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Session

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

conf.SERVER_MAIN_PASSWORD = (
    "7386ee647d14852db417a0eacb46c0499909aee90671395cb5e7a2f861f68ca1"
)

# this is for client
os.environ["PATH_PRIVATE_KEY"] = os.environ.get(
    "PATH_PRIVATE_KEY", str(os.path.join("tests", "private_key.pem"))
)


def create_dirs() -> None:
    os.makedirs(conf.STORAGE_ARTIFACTS, exist_ok=True)
    os.makedirs(conf.STORAGE_CLIENTS, exist_ok=True)
    os.makedirs(conf.STORAGE_MODELS, exist_ok=True)

    if os.path.exists(db_path):
        os.remove(db_path)


def delete_dirs() -> None:
    shutil.rmtree(conf.STORAGE_ARTIFACTS)
    shutil.rmtree(conf.STORAGE_CLIENTS)
    shutil.rmtree(conf.STORAGE_MODELS)

    os.remove(db_path)


@pytest.fixture()
def connection() -> Generator[Connection, None, None]:
    """This will be executed once each test and it will create a new database on a sqlite local file.
    The database will be used as the server's database and it will be populate this database with the required tables.
    """

    create_dirs()

    engine = create_engine(conf.db_connection_url(True))

    with engine.connect() as conn:
        Base.metadata.create_all(conn, checkfirst=True)
        try:
            yield conn
        finally:
            Base.metadata.drop_all(conn)
            delete_dirs()


@pytest.fixture()
def session(connection: Connection) -> Generator[Session, None, None]:
    with Session(connection) as session:
        yield session


@pytest_asyncio.fixture()
async def async_session() -> AsyncGenerator[AsyncSession, None]:

    create_dirs()

    inst = DataBase()

    async with inst.engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
        try:
            async with inst.session() as session:
                yield session
        finally:
            await conn.run_sync(Base.metadata.drop_all)
            delete_dirs()


@pytest.fixture()
def exchange() -> Exchange:
    exc = Exchange()
    exc.generate_key()

    return exc
