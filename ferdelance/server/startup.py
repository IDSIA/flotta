from ferdelance.config import conf
from ferdelance.database.const import PUBLIC_KEY
from ferdelance.database.data import COMPONENT_TYPES, TYPE_SERVER
from ferdelance.database.repositories import (
    Repository,
    AsyncSession,
    ComponentRepository,
    KeyValueStore,
    ProjectRepository,
)
from ferdelance.database.repositories.settings import setup_settings
from ferdelance.server import security

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
        await aiofiles.os.makedirs(conf.STORAGE_RESULTS, exist_ok=True)

        LOGGER.info("directory initialization completed")

    async def create_project(self) -> None:
        try:
            await self.pr.create_project("Project Zero", conf.PROJECT_DEFAULT_TOKEN)

        except ValueError:
            LOGGER.warning("Project zero already exists")

    async def init_security(self) -> None:
        LOGGER.info("setup setting and security keys")
        await setup_settings(self.session)
        await security.generate_keys(self.session)
        LOGGER.info("setup setting and security keys completed")

    async def populate_database(self) -> None:
        # server component
        public_key: str = await self.kvs.get_str(PUBLIC_KEY)
        await self.cr.create_types(COMPONENT_TYPES)
        try:
            await self.cr.create_component(TYPE_SERVER, public_key, "localhost")
            LOGGER.info("self component created")
        except ValueError:
            LOGGER.warning("self component already exists")

        # projects
        await self.create_project()

    async def startup(self) -> None:
        await self.init_directories()
        await self.init_security()
        await self.populate_database()
