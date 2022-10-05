
from . import security
from .. import __version__
from ..config import STORAGE_ARTIFACTS, STORAGE_CLIENTS, STORAGE_MODELS
from ..database import Session, settings, startup
from ..database.services import ClientService
from ..database.tables import Client, ClientToken
from ..server.services import SecurityService

import os
import platform
import uuid


class ServerStartup:
    def __init__(self, db: Session) -> None:
        self.db: Session = db
        self.cs: ClientService = ClientService(db)
        self.ss: SecurityService = SecurityService(db, None)

    def init_directories(self) -> None:
        os.makedirs(STORAGE_ARTIFACTS, exist_ok=True)
        os.makedirs(STORAGE_CLIENTS, exist_ok=True)
        os.makedirs(STORAGE_MODELS, exist_ok=True)

    def create_client(self, type: str, ip_address: str = '', system: str = '', mac_address: str = '') -> None:

        entry_exists = self.db.query(Client).filter(Client.client_id == type).first()

        if entry_exists is not None:
            return

        node: str = uuid.getnode()

        client_token: ClientToken = self.ss.generate_token(system, mac_address, node)

        client = Client(
            client_id=type,
            version=__version__,
            public_key='',
            machine_system=system,
            machine_mac_address=mac_address,
            machine_node=node,
            ip_address=ip_address,
            type=type,
        )

        self.cs.create_client(client)
        self.cs.create_client_token(client_token)

    def startup(self) -> None:
        self.init_directories()

        startup.init_database(self.db)
        settings.setup_settings(self.db)
        security.generate_keys(self.db)

        self.create_client(
            'SERVER',
            'localhost',
            platform.system(),
        )
        self.create_client(
            'WORKER',
        )
        self.create_client(
            'WORKBENCH',
        )

        self.db.commit()
