from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session

import os


Base = declarative_base()


def SessionLocal(database_url: str = None) -> Session:
    if database_url is None:
        database_url = os.environ.get('DATABASE_URL')

    engine = create_engine(database_url)
    session_maker = sessionmaker(autocommit=False, autoflush=False, bind=engine)

    return session_maker()


def get_db() -> Session:
    """This is a generator to get a session to the database through SQLAlchemy."""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
