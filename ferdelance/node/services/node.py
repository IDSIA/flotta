from ferdelance.config import config_manager
from ferdelance.const import TYPE_NODE
from ferdelance.logging import get_logger
from ferdelance.database import AsyncSession
from ferdelance.database.repositories import (
    ComponentRepository,
    DataSourceRepository,
    ProjectRepository,
)
from ferdelance.schemas.components import Component
from ferdelance.schemas.metadata import Metadata
from ferdelance.schemas.node import JoinData, NodeJoinRequest, NodeMetadata
from ferdelance.security.exchange import Exchange

from pathlib import Path
from sqlalchemy.exc import NoResultFound

import httpx


LOGGER = get_logger(__name__)


class NodeService:
    def __init__(self, session: AsyncSession, self_component: Component) -> None:
        self.session: AsyncSession = session

        self.self_component: Component = self_component

        private_key_path: Path = config_manager.get().private_key_location()
        self.exc: Exchange = Exchange(self_component.id, private_key_path=private_key_path)

        self.cr: ComponentRepository = ComponentRepository(self.session)

    async def connect(self, data: NodeJoinRequest, ip_address: str) -> JoinData:
        """
        :raise:
            NoResultFound if the access parameters (token) is not valid.

            SLQAlchemyError if there are issues with the creation of a new user in the database.

            ValueError if the given client data are incomplete or wrong.

        :return:
            A WorkbenchJoinData object that can be returned to the connected workbench.
        """
        try:
            await self.cr.get_by_key(data.public_key)

            raise ValueError("Invalid client data")

        except NoResultFound:
            pass
        except Exception as e:
            raise e

        LOGGER.info(f"component={data.id}: joining procedure start")

        component = await self.cr.create_component(
            data.id,
            data.type_name,
            data.public_key,
            data.version,
            data.name,
            ip_address,
            data.url,
        )

        self.exc.set_remote_key(data.id, data.public_key)

        LOGGER.info(f"component={component.id}: created as new {data.type_name}")

        self_component = await self.cr.get_self_component()

        nodes: list[Component] = list()

        if data.type_name == TYPE_NODE:
            saved_nodes = await self.cr.list_nodes()

            for node in saved_nodes:  # TODO: complete with list of nodes
                if node.id not in (data.id, self_component.id):
                    nodes.append(node)

        await self.distribute_add(component, nodes)

        nodes.append(self_component)

        LOGGER.info(f"component={data.id}: joining procedure done")

        return JoinData(
            component_id=self_component.id,
            nodes=nodes,
        )

    async def leave(self, component: Component) -> None:
        """
        :raise:
            NoResultFound when there is no project with the given token.
        """
        await self.cr.component_leave(component.id)
        await self.distribute_remove(component)

        LOGGER.info(f"component={component.id}: left")

    async def metadata(self, component: Component, metadata: Metadata) -> Metadata:
        dsr: DataSourceRepository = DataSourceRepository(self.session)
        pr: ProjectRepository = ProjectRepository(self.session)

        LOGGER.info(f"component={self.self_component.id}: metadata updating")

        # this will also update existing metadata
        await dsr.create_or_update_from_metadata(component.id, metadata)
        await pr.add_datasources_from_metadata(metadata)

        return metadata

    async def add(self, new_component: Component) -> None:
        try:
            await self.cr.get_by_id(new_component.id)
            LOGGER.warning(f"component={self.self_component.id}: new component={new_component.id} already exists")
            return
        except NoResultFound:
            pass

        try:
            c = await self.cr.get_by_key(new_component.public_key)
            LOGGER.warning(
                f"component={self.self_component.id}: public key already exists for new component={new_component.id} "
                f"under component={c.id}"
            )
            return
        except NoResultFound:
            pass

        await self.cr.create_component(
            new_component.id,
            new_component.type_name,
            new_component.public_key,
            new_component.version,
            new_component.name,
            new_component.ip_address,
            new_component.url,
        )

    async def remove(self, component: Component) -> None:
        await self.cr.component_leave(component.id)

    async def distribute_add(self, new_component: Component, nodes: list[Component]) -> None:
        if new_component.type_name != TYPE_NODE:
            return

        for node in nodes:
            if node.id not in (self.self_component.id, new_component.id):
                # skip self node
                continue

            if not node.active or node.blacklisted:
                # skip disabled nodes
                continue

            if node.type_name != TYPE_NODE:
                # skip nodes that are not server nodes
                continue

            LOGGER.info(f"component={self.self_component.id}: distributing node add to component={node.id}")

            self.exc.set_remote_key(node.id, node.public_key)

            headers, payload = self.exc.create(new_component.json())

            res = httpx.put(
                f"{node.url}/node/add",
                headers=headers,
                content=payload,
            )

            if res.status_code != 200:
                LOGGER.error(
                    f"component={self.self_component.id}: could not add component={new_component.id} to node={node.id}"
                )

    async def distribute_remove(self, component: Component) -> None:
        if component.type_name != TYPE_NODE:
            return

        for node in await self.cr.list_nodes():
            if node.id == self.self_component.id:
                # skip self node
                continue

            if node.type_name != TYPE_NODE:
                # skip nodes that are not server nodes
                continue

            headers, payload = self.exc.create(component.json())

            res = httpx.put(
                f"{node.url}/node/remove",
                headers=headers,
                content=payload,
            )
            if res.status_code != 200:
                LOGGER.error(
                    f"component={self.self_component.id}: could not remove "
                    f"component={component.id} from node={node.id}"
                )

    async def distribute_metadata(self, metadata: Metadata) -> None:
        # TODO: not used at the moment, how do we want to distribute metadata between NODES? (not clients!)

        node_metadata = NodeMetadata(id=self.self_component.id, metadata=metadata)

        for node in await self.cr.list_nodes():
            if node.id == self.self_component.id:
                # skip self node
                continue

            if node.type_name != TYPE_NODE:
                # skip nodes that are not server nodes
                continue

            LOGGER.info(f"component={self.self_component.id}: sending metadata to component={node.id}")

            self.exc.set_remote_key(node.id, node.public_key)

            headers, payload = self.exc.create(node_metadata.json())

            res = httpx.put(
                f"{node.url}/node/metadata",
                headers=headers,
                content=payload,
            )

            if res.status_code != 200:
                LOGGER.error(f"component={self.self_component.id}: could not send metadata to node={node.id}")
