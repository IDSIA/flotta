from . import security
from .. import __version__
from ..config import STORAGE_ARTIFACTS, STORAGE_CLIENTS, STORAGE_MODELS
from ..database import Session, settings, startup
from ..database.services import ClientService
from ..database.tables import Client, ClientToken
from ..server.services import SecurityService

import logging
import os
import platform
import re
import uuid

LOGGER = logging.getLogger(__name__)


class ServerStartup:
    def __init__(self, db: Session) -> None:
        self.db: Session = db
        self.cs: ClientService = ClientService(db)
        self.ss: SecurityService = SecurityService(db, None)

    def init_directories(self) -> None:
        LOGGER.info('directory initialization')

        os.makedirs(STORAGE_ARTIFACTS, exist_ok=True)
        os.makedirs(STORAGE_CLIENTS, exist_ok=True)
        os.makedirs(STORAGE_MODELS, exist_ok=True)

        LOGGER.info('directory initialization completed')

    def create_client(self, type: str, ip_address: str = '', system: str = '', node: int | None = None) -> None:
        LOGGER.info(f'creating client {type}')

        entry_exists = self.db.query(Client).filter(Client.client_id == type).first()

        if entry_exists is not None:
            LOGGER.warn(f'client already exists for type={type} ip_address={ip_address} system={system}')
            return

        if node is None:
            node = uuid.uuid4()

        node_str: str = str(node)[:12]
        mac_address: str = ':'.join(re.findall('..', f'{int(node):012x}'[:12]))

        client_token: ClientToken = self.ss.generate_token(system, mac_address, node_str)

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

        self.cs.create_client(client)
        self.cs.create_client_token(client_token)

        LOGGER.info(f'client {type} created')

    def init_database(self) -> None:
        startup.init_database(self.db)

    def init_security(self) -> None:
        LOGGER.info('setup setting and security keys')
        settings.setup_settings(self.db)
        security.generate_keys(self.db)
        LOGGER.info('setup setting and security keys completed')

    def populate_database(self) -> None:
        self.create_client(
            'SERVER',
            'localhost',
            platform.system(),
            uuid.getnode(),
        )
        self.create_client(
            'WORKER',
        )
        self.create_client(
            'WORKBENCH',
        )

        self.db.commit()

    def startup(self) -> None:
        self.init_directories()
        self.init_database()
        self.init_security()

        self.populate_database()
