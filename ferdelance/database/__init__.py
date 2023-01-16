from __future__ import annotations

from ferdelance.config import conf

from typing import Any, AsyncGenerator

from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, create_async_engine
from sqlalchemy.orm.decl_api import DeclarativeMeta
from sqlalchemy.orm import sessionmaker, registry

import logging


LOGGER = logging.getLogger(__name__)


mapper_registry = registry()


class Base(metaclass=DeclarativeMeta):
    __abstract__ = True

    registry = mapper_registry
    metadata = mapper_registry.metadata

    __init__ = mapper_registry.constructor


class DataBase:
    def __init__(self) -> None:
        self.database_url: str | None
        self.engine: AsyncEngine
        self.async_session_factory: Any
        self.async_session: Any

    def __new__(cls: type[DataBase]) -> DataBase:
        if not hasattr(cls, "instance"):
            LOGGER.debug("Database singleton creation")
            cls.instance = super(DataBase, cls).__new__(cls)

            cls.instance.database_url = conf.db_connection_url()

            cls.instance.engine = create_async_engine(cls.instance.database_url)
            cls.instance.async_session = sessionmaker(
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
