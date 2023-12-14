from typing import AsyncGenerator

from ferdelance.logging import get_logger
from ferdelance.database import AsyncSession
from ferdelance.const import TYPE_USER
from ferdelance.core.artifacts import ArtifactStatus, Artifact
from ferdelance.database.repositories import (
    ArtifactRepository,
    ComponentRepository,
    DataSourceRepository,
    ProjectRepository,
    ResourceRepository,
)
from ferdelance.schemas.client import ClientDetails
from ferdelance.schemas.components import Component
from ferdelance.schemas.database import Resource
from ferdelance.schemas.project import Project
from ferdelance.schemas.workbench import (
    WorkbenchClientList,
    WorkbenchDataSourceIdList,
    WorkbenchJoinRequest,
)
from ferdelance.node.services import JobManagementService

from sqlalchemy.exc import NoResultFound

import os

LOGGER = get_logger(__name__)


class WorkbenchConnectService:
    def __init__(self, session: AsyncSession) -> None:
        self.session: AsyncSession = session

    async def register(self, data: WorkbenchJoinRequest, ip_address: str) -> None:
        """Connects a workbench to the server. If the provided user_public_key
        does not exits, then a new user will be created.

        :param user_public_key:
            String containing the public key received from a workbench.

        :raise:
            NoResultFound if the access parameters (token) is not valid.

            SLQAlchemyError if there are issues with the creation of a new user in the database.

            ValueError if the given client data are incomplete or wrong.

        :return:
            A WorkbenchJoinData object that can be returned to the connected workbench.
            Note that the returned object does not have the server_public_key parameter set!
        """
        cr: ComponentRepository = ComponentRepository(self.session)

        try:
            user = await cr.get_by_key(data.public_key)

        except NoResultFound:
            # creating new user
            user = await cr.create_component(
                data.id,
                TYPE_USER,
                data.public_key,
                data.version,
                data.name,
                ip_address,
                "",
            )

            LOGGER.info(f"user={user.id}: created new user")

        LOGGER.info(f"user={user.id}: new workbench connected")


class WorkbenchService:
    def __init__(
        self,
        session: AsyncSession,
        wb_component: Component,
        self_component: Component,
        private_key: str = "",
        node_public_key: str = "",
    ) -> None:
        self.session: AsyncSession = session
        self.wb_component: Component = wb_component
        self.self_component: Component = self_component

        self.jms: JobManagementService = JobManagementService(
            self.session,
            self.self_component,
            private_key,
            node_public_key,
        )

    async def project(self, project_token: str) -> Project:
        """
        :raise:
            NoResultFound when there is no project with the given token.
        """
        pr: ProjectRepository = ProjectRepository(self.session)

        project = await pr.get_by_token(project_token)

        LOGGER.info(f"user={self.wb_component.id}: loaded project with project={project.id}")

        return project

    async def get_client_list(self, project_token: str) -> WorkbenchClientList:
        cr: ComponentRepository = ComponentRepository(self.session)
        pr: ProjectRepository = ProjectRepository(self.session)

        client_ids = await pr.list_client_ids(project_token)

        clients = await cr.list_clients_by_ids(client_ids)

        client_details = [ClientDetails(**c.dict()) for c in clients]

        LOGGER.info(
            f"user={self.wb_component.id}: found {len(client_details)} datasource(s) with token={project_token}"
        )

        return WorkbenchClientList(clients=client_details)

    async def get_datasource_list(self, project_token: str) -> WorkbenchDataSourceIdList:
        dsr: DataSourceRepository = DataSourceRepository(self.session)
        pr: ProjectRepository = ProjectRepository(self.session)

        datasource_ids = await pr.list_datasources_ids(project_token)

        datasources = [await dsr.load(ds_id) for ds_id in datasource_ids]

        LOGGER.info(f"user={self.wb_component.id}: found {len(datasources)} datasource(s) with token={project_token}")
        return WorkbenchDataSourceIdList(datasources=datasources)

    async def submit_artifact(self, artifact: Artifact) -> ArtifactStatus:
        return await self.jms.submit_artifact(artifact)

    async def store_resource(self, request_stream: AsyncGenerator[bytes, None]) -> str:
        return await self.jms.store_resource(request_stream)  # TODO: FIXME

    async def get_status_artifact(self, artifact_id: str) -> ArtifactStatus:
        """
        :raise:
            NoResultFound if the artifact was not found in the database.
        """
        ar: ArtifactRepository = ArtifactRepository(self.session)

        status = await ar.get_status(artifact_id)

        LOGGER.info(f"user={self.wb_component.id}: got status={status.status} for artifact={artifact_id} ")

        return status

    async def get_artifact(self, artifact_id: str) -> Artifact:
        """
        :raise:
            ValueError if the requested artifact cannot be found.
        """
        LOGGER.info(f"user={self.wb_component.id}: downloading artifact with artifact={artifact_id}")

        ar: ArtifactRepository = ArtifactRepository(self.session)

        artifact: Artifact = await ar.load(artifact_id)

        return artifact

    async def get_resource(self, resource_id: str) -> Resource:
        """
        :raise:
            ValueError when the requested resource exists on the database but not on disk.

            NoResultFound when there are no resources.

            MultipleResultsFound when the resource is not unique (database error).
        """
        rr: ResourceRepository = ResourceRepository(self.session)

        resource: Resource = await rr.get_by_id(resource_id)

        if not os.path.exists(resource.path):
            raise ValueError(f"resource={resource.id} not found at path={resource.path}")

        LOGGER.info(f"user={self.wb_component.id}: downloaded resource={resource_id}")

        return resource

    async def list_resources(self, artifact_id: str) -> list[Resource]:
        rr: ResourceRepository = ResourceRepository(self.session)

        return await rr.list_resources_by_artifact_id(artifact_id)
