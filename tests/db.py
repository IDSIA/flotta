from ferdelance.database import Base
from ferdelance.config import conf

from sqlalchemy import create_engine
from sqlalchemy.orm import close_all_sessions

import logging
import os

LOGGER = logging.getLogger(__name__)


def database_create():
    """Creates a new database on the remote server specified by `DB_HOST`, `DB_USER`, and `DB_PASS` (all env variables.).
    The name of the database is randomly generated using UUID4, if not supplied via `DB_SCHEMA` env variable.
    The database will be used as the server's database.
    """
    LOGGER.info('start setup module test database')

    DB_HOST = os.environ['DB_HOST']
    DB_USER = os.environ['DB_USER']
    DB_PASS = os.environ['DB_PASS']
    DB_SCHEMA = os.environ['DB_SCHEMA']

    # database
    db_string_no_db = f'postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}/postgres'

    with create_engine(db_string_no_db, isolation_level='AUTOCOMMIT').connect() as conn:
        try:
            conn.exec_driver_sql(f'CREATE DATABASE {DB_SCHEMA}')
            conn.exec_driver_sql(f'GRANT ALL PRIVILEGES ON DATABASE {DB_SCHEMA} to {DB_USER};')
        except Exception as _:
            LOGGER.warning('database already exists')

    LOGGER.info(f'created test database {DB_SCHEMA}')

    db_string = f'postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}/{DB_SCHEMA}'

    os.environ['DATABASE_URL_NO_DB'] = db_string_no_db
    os.environ['DATABASE_URL'] = db_string

    LOGGER.info('initialize database')

    with create_engine(db_string).connect() as conn:
        with conn.begin():
            Base.metadata.create_all(conn, checkfirst=True)

            os.makedirs(conf.STORAGE_ARTIFACTS, exist_ok=True)
            os.makedirs(conf.STORAGE_CLIENTS, exist_ok=True)
            os.makedirs(conf.STORAGE_MODELS, exist_ok=True)

    print('\nsetup module completed\n')


def database_delete():
    """Class teardown. This method will ensure that the database is closed and deleted from the remote dbms.
    Note that all database connections still open will be forced to close by this method.
    """
    LOGGER.info('teardown module started')

    close_all_sessions()
    LOGGER.info('database sessions closed')

    DB_SCHEMA = os.environ['DB_SCHEMA']

    # database
    db_string_no_db = os.environ.get('DATABASE_URL_NO_DB', None)

    assert db_string_no_db is not None

    with create_engine(db_string_no_db, isolation_level='AUTOCOMMIT').connect() as conn:
        with conn.begin():
            conn.exec_driver_sql(
                f"SELECT pg_terminate_backend(pg_stat_activity.pid) FROM pg_stat_activity WHERE pg_stat_activity.datname = '{DB_SCHEMA}' AND pid <> pg_backend_pid()")
            conn.exec_driver_sql(f'DROP DATABASE {DB_SCHEMA}')

    LOGGER.info(f'database {DB_SCHEMA} deleted')

    print('\nteardown module completed\n')
