from typing import AsyncGenerator

from asyncio import current_task

from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, AsyncEngine, async_scoped_session
from sqlalchemy.orm import sessionmaker, registry
from sqlalchemy.orm.decl_api import DeclarativeMeta

import os
import logging

LOGGER = logging.getLogger(__name__)


mapper_registry = registry()


class Base(metaclass=DeclarativeMeta):
    __abstract__ = True

    registry = mapper_registry
    metadata = mapper_registry.metadata

    __init__ = mapper_registry.constructor


class DataBase:
    _instance = None

    def __init__(self) -> None:
        self.database_url = os.environ.get('DATABASE_URL', None)

        assert self.database_url is not None

        self.engine: AsyncEngine = create_async_engine(self.database_url)

        self.async_session_factory = sessionmaker(self.engine, class_=AsyncSession, expire_on_commit=False, autocommit=False, autoflush=False)
        self.async_session = async_scoped_session(self.async_session_factory, scopefunc=current_task)

        LOGGER.info('DataBase factories created')

    @classmethod
    def instance(cls):
        if cls._instance is None:
            LOGGER.info('DataBase singleton created')
            cls._instance = cls.__new__(cls)

        return cls._instance


async def get_session() -> AsyncGenerator[AsyncSession, None]:
    async with DataBase.instance().async_session() as session:
        yield session
