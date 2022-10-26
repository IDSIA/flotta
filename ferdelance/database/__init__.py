from __future__ import annotations
from curses import echo
from typing import AsyncGenerator, Any

from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, AsyncEngine, async_session
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
    def __init__(self) -> None:
        self.database_url: str | None
        self.engine: AsyncEngine
        self.async_session_factory: Any
        self.async_session: Any
        LOGGER.info('init!!!')
        pass

    def __new__(cls: type[DataBase]) -> DataBase:
        if not hasattr(cls, 'instance'):
            LOGGER.info('singleton new')
            cls.instance = super(DataBase, cls).__new__(cls)

            DB_HOST = os.environ.get('DB_HOST', None)
            DB_USER = os.environ.get('DB_USER', None)
            DB_PASS = os.environ.get('DB_PASS', None)
            DB_SCHEMA = os.environ.get('DB_SCHEMA', None)

            assert DB_HOST is not None
            assert DB_USER is not None
            assert DB_PASS is not None
            assert DB_SCHEMA is not None

            cls.instance.database_url = f'postgresql+asyncpg://{DB_USER}:{DB_PASS}@{DB_HOST}/{DB_SCHEMA}'

            cls.instance.engine = create_async_engine(cls.instance.database_url)
            cls.instance.async_session = sessionmaker(bind=cls.instance.engine, class_=AsyncSession, expire_on_commit=False, autocommit=False, autoflush=False)

            LOGGER.info('DataBase factories created')

        return cls.instance


async def get_session() -> AsyncGenerator[AsyncSession, None]:
    db = DataBase()
    async with db.async_session() as session:
        yield session
