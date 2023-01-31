from ferdelance import __version__
from ferdelance.config import conf
from ferdelance.database.const import PUBLIC_KEY
from ferdelance.database.data import COMPONENT_TYPES, TYPE_SERVER, TYPE_WORKER
from ferdelance.database.services import DBSessionService, AsyncSession, ComponentService, KeyValueStore, ProjectService
from ferdelance.database.services.settings import setup_settings
from ferdelance.server import security

from sqlalchemy.exc import NoResultFound

import aiofiles.os
import logging

LOGGER = logging.getLogger(__name__)


class ServerStartup(DBSessionService):
    def __init__(self, session: AsyncSession) -> None:
        super().__init__(session)
        self.cs: ComponentService = ComponentService(session)
        self.kvs = KeyValueStore(session)
        self.ps: ProjectService = ProjectService(session)

    async def init_directories(self) -> None:
        LOGGER.info("directory initialization")

        await aiofiles.os.makedirs(conf.STORAGE_ARTIFACTS, exist_ok=True)
        await aiofiles.os.makedirs(conf.STORAGE_CLIENTS, exist_ok=True)
        await aiofiles.os.makedirs(conf.STORAGE_MODELS, exist_ok=True)

        LOGGER.info("directory initialization completed")

    async def create_component(self, type: str, public_key: str) -> None:
        LOGGER.info(f"creating component {type}")

        try:
            await self.cs.create(public_key=public_key, type_name=type)

        except ValueError:
            LOGGER.warning(f"client already exists for type={type}")
            return

        LOGGER.info(f"client {type} created")

    async def create_default_project(self) -> None:
        try:
            await self.ps.get_by_token(conf.PROJECT_DEFAULT_TOKEN)
            LOGGER.info("Default project already exists")

        except NoResultFound as _:
            try:
                p = await self.ps.get_by_name("Project Zero")
                await self.ps.update_token(p, conf.PROJECT_DEFAULT_TOKEN)

                LOGGER.info("Updated token of default project")

            except NoResultFound as _:
                await self.ps.create("Project Zero", conf.PROJECT_DEFAULT_TOKEN)
                LOGGER.info("Created default project")

    async def init_security(self) -> None:
        LOGGER.info("setup setting and security keys")
        await setup_settings(self.session)
        await security.generate_keys(self.session)
        LOGGER.info("setup setting and security keys completed")

    async def populate_database(self) -> None:
        spk: str = await self.kvs.get_str(PUBLIC_KEY)
        await self.cs.create_types(COMPONENT_TYPES)
        await self.create_component(TYPE_SERVER, spk)
        await self.create_component(TYPE_WORKER, "")  # TODO: worker should have a public key
        await self.create_default_project()
        await self.session.commit()

    async def startup(self) -> None:
        await self.init_directories()
        await self.init_security()
        await self.populate_database()
