from typing import AsyncGenerator

from ferdelance.config import config_manager
from ferdelance.const import COMPONENT_TYPES
from ferdelance.database import Base, DataBase
from ferdelance.database.tables import ComponentType

from .utils import TEST_PROJECT_TOKEN

from sqlalchemy.ext.asyncio import AsyncSession

from pathlib import Path

import os
import pytest_asyncio
import shutil


db_file = "./tests/test_sqlite.db"
db_path = Path(".") / db_file

conf = config_manager.get()

conf.database.memory = False
conf.database.dialect = "sqlite"
conf.database.host = db_file

conf.node.main_password = "7386ee647d14852db417a0eacb46c0499909aee90671395cb5e7a2f861f68ca1"
conf.node.token_project_default = TEST_PROJECT_TOKEN
conf.workdir = os.path.join("tests", "storage")

conf.node.allow_resource_download = True

conf.dump()

config_manager.setup()


def create_dirs() -> None:
    os.makedirs(conf.storage_datasources_dir(), exist_ok=True)
    os.makedirs(conf.storage_artifact_dir(), exist_ok=True)
    os.makedirs(conf.storage_clients_dir(), exist_ok=True)

    if os.path.exists(db_path):
        os.remove(db_path)


def delete_dirs() -> None:
    shutil.rmtree(conf.storage_datasources_dir())
    shutil.rmtree(conf.storage_artifact_dir())
    shutil.rmtree(conf.storage_clients_dir())

    if os.path.exists(db_path):
        os.remove(db_path)


@pytest_asyncio.fixture()
async def session() -> AsyncGenerator[AsyncSession, None]:
    create_dirs()

    inst = DataBase()

    async with inst.engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
        try:
            async with inst.session() as session:
                try:
                    for t in COMPONENT_TYPES:
                        session.add(ComponentType(type=t))
                    await session.commit()
                except Exception:
                    pass
                yield session
        except Exception as e:
            print(e)
        finally:
            await conn.run_sync(Base.metadata.drop_all)
            delete_dirs()
