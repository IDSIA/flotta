from ferdelance import __version__
from ferdelance.config import conf
from ferdelance.database.const import PUBLIC_KEY
from ferdelance.database.data import COMPONENT_TYPES, TYPE_SERVER, TYPE_WORKER
from ferdelance.database.repositories import (
    Repository,
    AsyncSession,
    ComponentRepository,
    KeyValueStore,
    ProjectRepository,
)
from ferdelance.database.repositories.settings import setup_settings
from ferdelance.server import security

from sqlalchemy.exc import NoResultFound

import aiofiles.os
import logging

LOGGER = logging.getLogger(__name__)


class ServerStartup(Repository):
    def __init__(self, session: AsyncSession) -> None:
        super().__init__(session)
        self.cr: ComponentRepository = ComponentRepository(session)
        self.kvs = KeyValueStore(session)
        self.pr: ProjectRepository = ProjectRepository(session)

    async def init_directories(self) -> None:
        LOGGER.info("directory initialization")

        await aiofiles.os.makedirs(conf.STORAGE_ARTIFACTS, exist_ok=True)
        await aiofiles.os.makedirs(conf.STORAGE_CLIENTS, exist_ok=True)
        await aiofiles.os.makedirs(conf.STORAGE_MODELS, exist_ok=True)

        LOGGER.info("directory initialization completed")

    async def create_component(self, type: str, public_key: str) -> None:
        LOGGER.info(f"creating component {type}")

        try:
            await self.cr.create(public_key=public_key, type_name=type)

        except ValueError:
            LOGGER.warning(f"client already exists for type={type}")
            return

        LOGGER.info(f"client {type} created")

    async def create_project(self) -> None:
        try:
            await self.pr.create("Project Zero", conf.PROJECT_DEFAULT_TOKEN)

        except ValueError:
            LOGGER.warning("Project zero already exists")

    async def init_security(self) -> None:
        LOGGER.info("setup setting and security keys")
        await setup_settings(self.session)
        await security.generate_keys(self.session)
        LOGGER.info("setup setting and security keys completed")

    async def populate_database(self) -> None:
        spk: str = await self.kvs.get_str(PUBLIC_KEY)
        await self.cr.create_types(COMPONENT_TYPES)
        await self.create_component(TYPE_SERVER, spk)
        await self.create_component(TYPE_WORKER, "")  # TODO: worker should have a public key
        await self.create_project()

    async def startup(self) -> None:
        await self.init_directories()
        await self.init_security()
        await self.populate_database()
