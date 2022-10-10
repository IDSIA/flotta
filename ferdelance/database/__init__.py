from typing import Generator
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session

import os


Base = declarative_base()


def SessionLocal(database_url: str | None = None) -> Session:
    if not database_url:
        database_url = os.environ.get('DATABASE_URL', None)

    assert database_url is not None

    engine = create_engine(database_url)
    session_maker = sessionmaker(autocommit=False, autoflush=False, bind=engine)

    return session_maker()


def get_db() -> Generator[Session, None, None]:
    """This is a generator to get a session to the database through SQLAlchemy."""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
