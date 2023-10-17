from typing import AsyncGenerator

from ferdelance.logging import get_logger
from ferdelance.database import AsyncSession
from ferdelance.const import TYPE_USER
from ferdelance.database.repositories import (
    ArtifactRepository,
    ComponentRepository,
    DataSourceRepository,
    ProjectRepository,
    ResourceRepository,
)
from ferdelance.schemas.artifacts import ArtifactStatus, Artifact
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
    def __init__(self, session: AsyncSession, component: Component) -> None:
        self.session: AsyncSession = session
        self.component: Component = component

    async def project(self, project_token: str) -> Project:
        """
        :raise:
            NoResultFound when there is no project with the given token.
        """
        pr: ProjectRepository = ProjectRepository(self.session)

        project = await pr.get_by_token(project_token)

        LOGGER.info(f"user={self.component.id}: loaded project with project={project.id}")

        return project

    async def get_client_list(self, project_token: str) -> WorkbenchClientList:
        cr: ComponentRepository = ComponentRepository(self.session)
        pr: ProjectRepository = ProjectRepository(self.session)

        client_ids = await pr.list_client_ids(project_token)

        clients = await cr.list_clients_by_ids(client_ids)

        client_details = [ClientDetails(**c.dict()) for c in clients]

        LOGGER.info(f"user={self.component.id}: found {len(client_details)} datasource(s) with token={project_token}")

        return WorkbenchClientList(clients=client_details)

    async def get_datasource_list(self, project_token: str) -> WorkbenchDataSourceIdList:
        dsr: DataSourceRepository = DataSourceRepository(self.session)
        pr: ProjectRepository = ProjectRepository(self.session)

        datasource_ids = await pr.list_datasources_ids(project_token)

        datasources = [await dsr.load(ds_id) for ds_id in datasource_ids]

        LOGGER.info(f"user={self.component.id}: found {len(datasources)} datasource(s) with token={project_token}")
        return WorkbenchDataSourceIdList(datasources=datasources)

    async def submit_artifact(self, artifact: Artifact) -> ArtifactStatus:
        jms: JobManagementService = JobManagementService(
            self.session, self.component
        )  # TODO: pass pub/priv key from args
        return await jms.submit_artifact(artifact)

    async def store_resource(self, request_stream: AsyncGenerator[bytes, None]) -> str:
        jms: JobManagementService = JobManagementService(
            self.session, self.component
        )  # TODO: pass pub/priv key from args
        return await jms.store_resource(request_stream)

    async def get_status_artifact(self, artifact_id: str) -> ArtifactStatus:
        """
        :raise:
            NoResultFound if the artifact was not found in the database.
        """
        ar: ArtifactRepository = ArtifactRepository(self.session)

        status = await ar.get_status(artifact_id)

        LOGGER.info(f"user={self.component.id}: got status={status.status} for artifact={artifact_id} ")

        return status

    async def get_artifact(self, artifact_id: str) -> Artifact:
        """
        :raise:
            ValueError if the requested artifact cannot be found.
        """
        LOGGER.info(f"user={self.component.id}: dowloading artifact with artifact={artifact_id}")

        ar: ArtifactRepository = ArtifactRepository(self.session)

        artifact: Artifact = await ar.load(artifact_id)

        return artifact

    async def get_resource(self, artifact_id: str) -> Resource:
        """
        :raise:
            ValueError when the requested resource exists on the database but not on disk.

            NoResultFound when there are no resources.

            MultipleResultsFound when the resource is not unique (database error).
        """
        rr: ResourceRepository = ResourceRepository(self.session)

        resource: Resource = await rr.get_aggregated_resource(artifact_id)

        if not os.path.exists(resource.path):
            raise ValueError(f"resource={resource.id} not found at path={resource.path}")

        LOGGER.info(f"user={self.component.id}: downloaded resources for artifact={artifact_id}")

        return resource

    async def get_partial_resource(self, artifact_id: str, builder_id: str, iteration: int) -> Resource:
        """
        :raise:
            ValueError when the requested partial resource exists on the database but not on disk.

            NoResultFound when there are no resources.

            MultipleResultsFound when the resource is not unique (database error).
        """

        rr: ResourceRepository = ResourceRepository(self.session)

        resource: Resource = await rr.get_partial_resource(artifact_id, builder_id, iteration)

        if not os.path.exists(resource.path):
            raise ValueError(f"partial resource={resource.id} not found at path={resource.path}")

        LOGGER.info(
            f"user={self.component.id}: downloaded partial resource for artifact={artifact_id} "
            f"and builder_user={builder_id}"
        )

        return resource
