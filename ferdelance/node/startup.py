from ferdelance import __version__
from ferdelance.config import config_manager, Configuration, DataSourceStorage
from ferdelance.const import TYPE_CLIENT, COMPONENT_TYPES, TYPE_NODE
from ferdelance.database.repositories import (
    Repository,
    AsyncSession,
    ComponentRepository,
    KeyValueStore,
    ProjectRepository,
)
from ferdelance.logging import get_logger
from ferdelance.node.services import NodeService
from ferdelance.node.services.security import SecurityService
from ferdelance.schemas.components import Component
from ferdelance.schemas.node import JoinData, NodeJoinRequest, NodePublicKey
from ferdelance.shared.checksums import str_checksum
from ferdelance.tasks.backends import get_jobs_backend

from sqlalchemy.exc import NoResultFound

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

        self.ss: SecurityService = SecurityService()

        self.config: Configuration = config_manager.get()

        LOGGER.debug(f"datasources found: {len(self.config.datasources)}")

        self.data: DataSourceStorage = DataSourceStorage(self.config.datasources)

        self.self_component: Component
        self.remote_key: str

    async def create_project(self) -> None:
        """Create teh initial project with the default token given through
        configuration files.
        """
        p_token = self.config.node.token_project_default
        try:
            await self.pr.create_project("Project Zero", p_token)
            LOGGER.info(f"component={self.self_component.id}: created project zero with token={p_token}")

        except ValueError:
            LOGGER.warning(f"component={self.self_component.id}: project zero already exists with token={p_token}")

    async def add_metadata(self) -> None:
        """Add metadata found in the configuration file. The metadata are
        extracted from the given data sources.
        """
        metadata = self.data.metadata()

        if not metadata.datasources:
            LOGGER.info(f"component={self.self_component.id}: no metadata associated with this node")
            return

        ns: NodeService = NodeService(self.session, self.self_component)

        await ns.metadata(metadata)
        await ns.distribute_metadata(metadata)

    async def populate_database(self) -> None:
        """Add basic information to the database."""

        # node self component
        await self.cr.create_types(COMPONENT_TYPES)
        try:
            self.self_component = await self.cr.get_self_component()
            LOGGER.warning("self component already exists")

        except NoResultFound:
            # define component id
            LOGGER.info("creating self component")

            component_id = str(
                uuid.uuid5(
                    uuid.NAMESPACE_URL,
                    f"{self.config.node.name}+{self.config.url_extern()}",
                )
            )

            self.self_component = await self.cr.create_component(
                component_id,
                TYPE_NODE,
                self.ss.get_public_key(),
                __version__,
                self.config.node.name,
                "127.0.0.1",
                self.config.url_extern(),
                True,
            )

        LOGGER.info(f"component={self.self_component.id}: id assigned")

        # projects
        await self.create_project()

    async def join(self) -> None:
        if self.config.join.first:
            LOGGER.info(f"component={self.self_component.id}: node defined as first, no join required")
            return

        if self.config.join.url is None:
            LOGGER.warning(f"component={self.self_component.id}: remote node url not set")
            return

        try:
            join_component = await self.cr.get_join_component()
            LOGGER.info(
                f"component={self.self_component.id}: node already joined to remote node "
                f"component={join_component.id} url={join_component.url}"
            )
            self.remote_key = join_component.public_key
            self.ss.set_remote_key(self.remote_key)

            return

        except NoResultFound:
            LOGGER.info(f"component={self.self_component.id}: starting join procedure")

        remote = self.config.join.url.rstrip("/")
        try:
            # get remote public key (this is also a check for valid node)
            res = requests.get(f"{remote}/node/key")

            res.raise_for_status()

            content = NodePublicKey(**res.json())
            self.remote_key = content.public_key
            self.ss.set_remote_key(self.remote_key)

            type_name = TYPE_NODE if self.config.mode == "node" else TYPE_CLIENT

            data_to_sign = f"{self.self_component.id}:{self.self_component.public_key}"

            checksum = str_checksum(data_to_sign)
            signature = self.ss.sign(data_to_sign)

            # send join data
            join_req = NodeJoinRequest(
                id=self.self_component.id,
                name=self.config.node.name,
                type_name=type_name,
                public_key=self.ss.get_public_key(),
                version=__version__,
                url=self.config.url_extern(),
                checksum=checksum,
                signature=signature,
            )

            _, join_req_payload = self.ss.exc.create_payload(join_req.json())
            headers = self.ss.exc.create_header(True)

            res = requests.post(
                f"{remote}/node/join",
                headers=headers,
                data=join_req_payload,
            )

            res.raise_for_status()

            _, payload = self.ss.exc.get_payload(res.content)

            # get node list
            join_data = JoinData(**json.loads(payload))

            LOGGER.info(f"component={self.self_component.id}: joined node component={join_data.component_id}")

            for node in join_data.nodes:
                # insert nodes into database
                LOGGER.info(
                    f"component={self.self_component.id}: adding new node with component={node.id} url={node.url}"
                )

                await self.cr.create_component(
                    node.id,
                    node.type_name,
                    node.public_key,
                    node.version,
                    node.name,
                    node.ip_address,
                    node.url,
                    False,
                    node.id == join_data.component_id,
                )

        except Exception as e:
            LOGGER.error(f"component={self.self_component.id}: could not join remote node at {remote}: {e}")
            return

    async def start_heartbeat(self):
        if self.config.mode in ("client", "standalone"):
            LOGGER.info(f"component={self.self_component.id}: starting client heartbeat")

            get_jobs_backend().start_heartbeat(
                self.self_component.id,
                self.remote_key,
            )

    async def startup(self) -> None:
        await self.populate_database()

        await self.join()

        await self.add_metadata()

        await self.start_heartbeat()
