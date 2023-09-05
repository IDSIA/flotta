from ferdelance import __version__
from ferdelance.config import config_manager, Configuration, DataSourceStorage
from ferdelance.client.state import State
from ferdelance.const import PUBLIC_KEY, PRIVATE_KEY, TYPE_CLIENT
from ferdelance.const import COMPONENT_TYPES, TYPE_NODE
from ferdelance.database.repositories import (
    Repository,
    AsyncSession,
    ComponentRepository,
    KeyValueStore,
    ProjectRepository,
)
from ferdelance.database.repositories.settings import setup_settings
from ferdelance.logging import get_logger
from ferdelance.node import security
from ferdelance.node.services import NodeService
from ferdelance.schemas.components import Component
from ferdelance.schemas.node import JoinData, NodeJoinRequest, ServerPublicKey
from ferdelance.shared.checksums import str_checksum
from ferdelance.shared.exchange import Exchange

from sqlalchemy.exc import NoResultFound

import aiofiles.os
import json
import requests
import uuid

LOGGER = get_logger(__name__)


class NodeStartup(Repository):
    def __init__(self, session: AsyncSession) -> None:
        super().__init__(session)
        self.cr: ComponentRepository = ComponentRepository(session)
        self.pr: ProjectRepository = ProjectRepository(session)
        self.kvs = KeyValueStore(session)

        self.config: Configuration = config_manager.get()

        LOGGER.debug(f"datasources found: {len(self.config.datasources)}")

        self.data: DataSourceStorage = DataSourceStorage(self.config.datasources)

        self.state: State = State(self.config)

        self.component: Component

    async def init_directories(self) -> None:
        LOGGER.info("directory initialization")

        await aiofiles.os.makedirs(self.config.storage_artifact_dir(), exist_ok=True)
        await aiofiles.os.makedirs(self.config.storage_clients_dir(), exist_ok=True)
        await aiofiles.os.makedirs(self.config.storage_results_dir(), exist_ok=True)

        LOGGER.info("directory initialization completed")

    async def create_project(self) -> None:
        try:
            await self.pr.create_project("Project Zero", self.config.node.token_project_default)

        except ValueError:
            LOGGER.warning("Project zero already exists")

    async def add_metadata(self) -> None:
        ns: NodeService = NodeService(self.session, self.component)

        await ns.metadata(self.data.metadata())

    async def init_security(self) -> None:
        LOGGER.info("setup setting and security keys")
        await setup_settings(self.session)
        await security.generate_keys(self.session)
        LOGGER.info("setup setting and security keys completed")

    async def populate_database(self) -> None:
        # node self component
        public_key: str = await self.kvs.get_str(PUBLIC_KEY)
        await self.cr.create_types(COMPONENT_TYPES)
        try:
            self.component = await self.cr.get_self_component()
            LOGGER.warning("self component already exists")

        except NoResultFound:
            # define component id
            component_id = str(
                uuid.uuid5(
                    uuid.NAMESPACE_URL,
                    self.config.node.url_extern(),
                )
            )

            self.component = await self.cr.create_component(
                component_id,
                TYPE_NODE,
                public_key,
                __version__,
                self.config.node.name,
                "127.0.0.1",
                self.config.node.url,
                True,
            )
            LOGGER.info("self component created")

        # projects
        await self.create_project()

        # metadata
        await self.add_metadata()

    async def join(self) -> None:
        if self.config.join.first:
            LOGGER.info("node defined as first, no join required")
            return

        if self.config.join.url is None:
            LOGGER.warning("remote node url not set")
            return

        remote = self.config.join.url.rstrip("/")
        try:
            # get remote public key (this is also a check for valid node)
            res = requests.get(f"{remote}/node/key")

            res.raise_for_status()

            content = ServerPublicKey(**res.json())
            private_bytes: bytes = await self.kvs.get_bytes(PRIVATE_KEY)

            exc = Exchange()
            exc.set_remote_key(content.public_key)
            exc.set_key_bytes(private_bytes)

            type_name = TYPE_NODE if self.config.mode == "node" else TYPE_CLIENT

            data_to_sign = f"{self.component.id}:{self.component.public_key}"

            checksum = str_checksum(data_to_sign)
            signature = exc.sign(data_to_sign)

            # send join data
            join_req = NodeJoinRequest(
                id=self.component.id,
                name=self.config.node.name,
                type_name=type_name,
                public_key=exc.transfer_public_key(),
                version=__version__,
                url=self.config.node.url_extern(),
                checksum=checksum,
                signature=signature,
            )

            headers, join_req_payload = exc.create(self.component.id, join_req.json())

            res = requests.post(
                f"{remote}/node/join",
                headers=headers,
                data=join_req_payload,
            )

            res.raise_for_status()

            _, payload = exc.get_payload(res.content)

            # get node list
            join_data = JoinData(**json.loads(payload))

            for node in join_data.nodes:
                # insert nodes into database
                await self.cr.create_component(
                    node.id,
                    node.type_name,
                    node.public_key,
                    node.version,
                    node.name,
                    node.ip_address,
                    node.url,
                    False,
                )

                # TODO: send metadata

            # TODO: save data (token, id) to disk
        except Exception as e:
            LOGGER.error(f"could not join remote node at {remote}: {e}")
            return

    async def startup(self) -> None:
        await self.init_directories()
        await self.init_security()
        await self.populate_database()

        await self.join()
