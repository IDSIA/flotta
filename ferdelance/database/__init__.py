from __future__ import annotations

from ferdelance.config import Configuration, DatabaseConfiguration, config_manager

from typing import Any, AsyncGenerator

from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, create_async_engine, async_sessionmaker
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.engine import URL

import logging


LOGGER = logging.getLogger(__name__)


def db_connection_url(conf: DatabaseConfiguration, sync: bool = False) -> URL | str:
    driver: str = ""

    if conf.memory:
        if not sync:
            driver = "+aiosqlite"

        return f"sqlite{driver}://"

    dialect = conf.dialect.lower()

    assert conf.host is not None

    if dialect == "sqlite":
        if not sync:
            driver = "+aiosqlite"

        # in this case host is an absolute path
        return f"sqlite{driver}:///{conf.host}"

    if dialect == "postgresql":
        assert conf.username is not None
        assert conf.password is not None
        assert conf.port is not None

        if not sync:
            driver = "+asyncpg"

        return URL.create(
            f"postgresql{driver}",
            conf.username,
            conf.password,
            conf.host,
            conf.port,
            conf.scheme,
        )

    raise ValueError(f"dialect {dialect} is not supported")


class Base(DeclarativeBase):
    pass


class DataBase:
    def __init__(self) -> None:
        self.database_url: URL | str | None
        self.engine: AsyncEngine
        self.async_session_factory: Any
        self.async_session: Any

    def __new__(cls: type[DataBase]) -> DataBase:
        if not hasattr(cls, "instance"):
            LOGGER.debug("Database singleton creation")
            cls.instance = super(DataBase, cls).__new__(cls)

            conf: Configuration = config_manager.get()
            cls.instance.database_url = db_connection_url(conf.database)

            if cls.instance.database_url is None:
                raise ValueError("Connection to database is not set!")

            cls.instance.engine = create_async_engine(cls.instance.database_url)
            cls.instance.async_session = async_sessionmaker(
                bind=cls.instance.engine,
                class_=AsyncSession,
                expire_on_commit=False,
                autocommit=False,
                autoflush=False,
            )

            LOGGER.info("DataBase connection established")

        return cls.instance

    def session(self) -> AsyncSession:
        return self.async_session()


async def get_session() -> AsyncGenerator[AsyncSession, None]:
    db = DataBase()
    async with db.session() as session:
        yield session
