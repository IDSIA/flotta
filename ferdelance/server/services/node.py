from ferdelance.config import get_logger
from ferdelance.database import AsyncSession
from ferdelance.database.repositories import (
    ComponentRepository,
    DataSourceRepository,
    ProjectRepository,
)
from ferdelance.schemas.components import Component, Client
from ferdelance.schemas.metadata import Metadata

from ferdelance.schemas.node import JoinData, JoinRequest

from sqlalchemy.exc import NoResultFound

LOGGER = get_logger(__name__)


class NodeService:
    def __init__(self, session: AsyncSession, component: Component | Client) -> None:
        self.session: AsyncSession = session
        self.component: Component | Client = component

    async def connect(self, client_public_key: str, data: JoinRequest, ip_address: str) -> JoinData:
        """
        :raise:
            NoResultFound if the access parameters (token) is not valid.

            SLQAlchemyError if there are issues with the creation of a new user in the database.

            ValueError if the given client data are incomplete or wrong.

        :return:
            A WorkbenchJoinData object that can be returned to the connected workbench.
        """
        cr: ComponentRepository = ComponentRepository(self.session)

        try:
            await cr.get_by_key(client_public_key)

            raise ValueError("Invalid client data")

        except NoResultFound:
            LOGGER.info("joining new client")
            # create new client
            client, token = await cr.create_client(
                name=data.name,
                version=data.version,
                public_key=client_public_key,
                machine_system=data.system,
                machine_mac_address=data.mac_address,
                machine_node=data.node,
                ip_address=ip_address,
            )

            LOGGER.info(f"client_id={client.id}: created new client")

            await cr.create_event(client.id, "creation")

        LOGGER.info(f"client_id={client.id}: created new client")

        return JoinData(
            id=client.id,
            token=token.token,
            public_key="",
        )

    async def leave(self) -> None:
        """
        :raise:
            NoResultFound when there is no project with the given token.
        """
        cr: ComponentRepository = ComponentRepository(self.session)
        await cr.client_leave(self.component.id)
        await cr.create_event(self.component.id, "left")

    async def metadata(self, metadata: Metadata) -> Metadata:
        cr: ComponentRepository = ComponentRepository(self.session)
        dsr: DataSourceRepository = DataSourceRepository(self.session)
        pr: ProjectRepository = ProjectRepository(self.session)

        await cr.create_event(self.component.id, "update metadata")

        # this will also update existing metadata
        await dsr.create_or_update_from_metadata(self.component.id, metadata)
        await pr.add_datasources_from_metadata(metadata)

        return metadata
