from ferdelance import __version__
from ferdelance.config import conf
from ferdelance.database.data import COMPONENT_TYPES
from ferdelance.database.services import DBSessionService, AsyncSession
from ferdelance.database.services import ComponentService
from ferdelance.database.services.settings import setup_settings
from ferdelance.database.tables import Token
from ferdelance.server import security
from ferdelance.server.services import SecurityService

import aiofiles.os
import logging

LOGGER = logging.getLogger(__name__)


class ServerStartup(DBSessionService):
    def __init__(self, session: AsyncSession) -> None:
        super().__init__(session)
        self.cs: ComponentService = ComponentService(session)
        self.ss: SecurityService = SecurityService(session)

    async def init_directories(self) -> None:
        LOGGER.info("directory initialization")

        await aiofiles.os.makedirs(conf.STORAGE_ARTIFACTS, exist_ok=True)
        await aiofiles.os.makedirs(conf.STORAGE_CLIENTS, exist_ok=True)
        await aiofiles.os.makedirs(conf.STORAGE_MODELS, exist_ok=True)

        LOGGER.info("directory initialization completed")

    async def create_component(self, type: str) -> None:
        LOGGER.info(f"creating component {type}")

        try:
            await self.cs.create(public_key="", type_name=type)

        except ValueError:
            LOGGER.warning(f"client already exists for type={type}")
            return

        LOGGER.info(f"client {type} created")

    async def init_security(self) -> None:
        LOGGER.info("setup setting and security keys")
        await setup_settings(self.session)
        await security.generate_keys(self.session)
        LOGGER.info("setup setting and security keys completed")

    async def populate_database(self) -> None:
        await self.cs.create_types(COMPONENT_TYPES)
        await self.create_component("SERVER")
        await self.create_component("WORKER")
        await self.session.commit()

    async def startup(self) -> None:
        await self.init_directories()
        await self.init_security()
        await self.populate_database()
