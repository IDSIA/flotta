from ferdelance.logging import get_logger
from ferdelance.database import AsyncSession
from ferdelance.database.repositories import (
    ComponentRepository,
    DataSourceRepository,
    ProjectRepository,
)
from ferdelance.node.services.security import SecurityService
from ferdelance.schemas.components import Component
from ferdelance.schemas.metadata import Metadata
from ferdelance.schemas.node import JoinData, NodeJoinRequest

from sqlalchemy.exc import NoResultFound

import requests


LOGGER = get_logger(__name__)


class NodeService:
    def __init__(self, session: AsyncSession, component: Component) -> None:
        self.session: AsyncSession = session
        self.component: Component = component

        self.ss: SecurityService = SecurityService(session)
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
            LOGGER.info(f"component_id={data.id}: joining new component")

            component = await self.cr.create_component(
                data.id,
                data.type_name,
                data.public_key,
                data.version,
                data.name,
                ip_address,
                data.url,
            )

            LOGGER.info(f"component_id={component.id}: created new client")

            await self.cr.create_event(component.id, "creation")

            await self.distribute_add(component)

        LOGGER.info(f"component_id={component.id}: created new client")

        self_component = await self.cr.get_self_component()

        return JoinData(
            component=self_component,
            nodes=list(),  # TODO: complete with list of nodes
        )

    async def leave(self) -> None:
        """
        :raise:
            NoResultFound when there is no project with the given token.
        """
        await self.cr.component_leave(self.component.id)
        await self.cr.create_event(self.component.id, "left")
        await self.distribute_remove(self.component)

    async def metadata(self, metadata: Metadata) -> Metadata:
        dsr: DataSourceRepository = DataSourceRepository(self.session)
        pr: ProjectRepository = ProjectRepository(self.session)

        await self.cr.create_event(self.component.id, "update metadata")

        # this will also update existing metadata
        await dsr.create_or_update_from_metadata(self.component.id, metadata)
        await pr.add_datasources_from_metadata(metadata)

        await self.distribute_metadata(self.component, metadata)

        return metadata

    async def distribute_add(self, component: Component) -> None:
        for node in await self.cr.list_nodes():
            if node.id == self.component.id:
                # skip self node
                continue

            requests.post(
                f"{node.url}/node/add",
                data=None,  # TODO: fill with encrypted component request
            )

    async def distribute_remove(self, component: Component) -> None:
        ...

    async def distribute_metadata(self, component: Component, metadata: Metadata) -> None:
        ...
