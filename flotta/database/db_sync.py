from __future__ import annotations

from typing import Any, Generator

from ferdelance.logging import get_logger

from .utils import db_connection_url

from sqlalchemy.engine import Engine, create_engine, URL
from sqlalchemy.orm import Session, sessionmaker


LOGGER = get_logger(__name__)


class DataBase:
    def __init__(self) -> None:
        self.database_url: URL | str | None
        self.engine: Engine
        self.session_factory: Any
        self.sync_session: Any

    def __new__(cls) -> DataBase:
        if not hasattr(cls, "instance"):
            LOGGER.debug("database singleton creation")
            cls.instance = super(DataBase, cls).__new__(cls)

            cls.instance.database_url = db_connection_url(True)

            if cls.instance.database_url is None:
                raise ValueError("Connection to database is not set!")

            cls.instance.engine = create_engine(cls.instance.database_url)
            cls.instance.sync_session = sessionmaker(
                bind=cls.instance.engine,
                class_=Session,
                expire_on_commit=False,
                autocommit=False,
                autoflush=False,
            )

            LOGGER.info("dataBase connection established")

        return cls.instance

    def session(self) -> Session:
        return self.sync_session()


def get_session() -> Generator[Session, None, None]:
    db = DataBase()
    with db.session() as session:
        yield session
