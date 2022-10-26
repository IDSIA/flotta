from . import security
from .. import __version__
from ..config import STORAGE_ARTIFACTS, STORAGE_CLIENTS, STORAGE_MODELS
from ..database.services import ClientService, DBSessionService, AsyncSession, setup_settings
from ..database.tables import Client, ClientToken
from ..server.services import SecurityService

from sqlalchemy import select

import logging
import os
import platform
import re
import uuid

LOGGER = logging.getLogger(__name__)


class ServerStartup(DBSessionService):
    def __init__(self, session: AsyncSession) -> None:
        super().__init__(session)
        self.cs: ClientService = ClientService(session)
        self.ss: SecurityService = SecurityService(session, None)

    async def init_directories(self) -> None:
        LOGGER.info('directory initialization')

        os.makedirs(STORAGE_ARTIFACTS, exist_ok=True)
        os.makedirs(STORAGE_CLIENTS, exist_ok=True)
        os.makedirs(STORAGE_MODELS, exist_ok=True)

        LOGGER.info('directory initialization completed')

    async def create_client(self, type: str, ip_address: str = '', system: str = '', node: int | None = None) -> None:
        LOGGER.info(f'creating client {type}')

        res = await self.session.execute(select(Client).where(Client.type == type).limit(1))
        entry_exists = res.scalar_one_or_none()

        if entry_exists is not None:
            LOGGER.warning(f'client already exists for type={type} ip_address={ip_address} system={system}')
            return

        if node is None:
            node = uuid.uuid4().int

        node_str: str = str(node)[:12]
        mac_address: str = ':'.join(re.findall('..', f'{node:012x}'[:12]))

        client_token: ClientToken = await self.ss.generate_token(system, mac_address, node_str)

        client = Client(
            client_id=client_token.client_id,
            version=__version__,
            public_key='',
            machine_system=system,
            machine_mac_address=mac_address,
            machine_node=node_str,
            ip_address=ip_address,
            type=type,
        )

        await self.cs.create_client(client)
        await self.cs.create_client_token(client_token)

        LOGGER.info(f'client {type} created')

    async def init_security(self) -> None:
        LOGGER.info('setup setting and security keys')
        await setup_settings(self.session)
        await security.generate_keys(self.session)
        LOGGER.info('setup setting and security keys completed')

    async def populate_database(self) -> None:
        await self.create_client(
            'SERVER',
            'localhost',
            platform.system(),
            uuid.getnode(),
        )
        await self.create_client(
            'WORKER',
        )
        await self.create_client(
            'WORKBENCH',
        )

        await self.session.commit()

    async def startup(self) -> None:
        await self.init_directories()
        await self.init_security()

        await self.populate_database()
