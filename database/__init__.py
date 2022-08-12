from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session

import os


DATABASE_URL = os.environ.get('DATABASE_URL')

engine = create_engine(DATABASE_URL)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

Base = declarative_base()

def get_db() -> Session:
    """This is a generator to get a session to the database through SQLAlchemy."""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
