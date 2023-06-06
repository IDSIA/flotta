from ferdelance.database import AsyncSession
from ferdelance.database.data import TYPE_USER
from ferdelance.database.repositories import (
    ArtifactRepository,
    ComponentRepository,
    DataSourceRepository,
    ProjectRepository,
    ResultRepository,
)
from ferdelance.jobs import job_manager, JobManagementService
from ferdelance.schemas.artifacts import (
    ArtifactStatus,
    Artifact,
)
from ferdelance.schemas.client import ClientDetails
from ferdelance.schemas.components import Component, Token
from ferdelance.schemas.database import Result
from ferdelance.schemas.project import Project
from ferdelance.schemas.workbench import (
    WorkbenchClientList,
    WorkbenchDataSourceIdList,
    WorkbenchJoinData,
    WorkbenchProjectToken,
    WorkbenchArtifact,
)

from sqlalchemy.exc import NoResultFound

import logging
import os

LOGGER = logging.getLogger(__name__)


class WorkbenchConnectService:
    def __init__(self, session: AsyncSession) -> None:
        self.session: AsyncSession = session

    async def connect(self, user_public_key: str) -> WorkbenchJoinData:
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
            user = await cr.get_by_key(user_public_key)

            try:
                token: Token = await cr.get_token_by_component_id(user.component_id)

            except NoResultFound as e:
                raise e

        except NoResultFound:
            # creating new user
            user, token = await cr.create_component(TYPE_USER, public_key=user_public_key)

            LOGGER.info(f"user_id={user.component_id}: created new user")

        LOGGER.info(f"user_id={user.component_id}: new workbench connected")

        return WorkbenchJoinData(
            id=user.component_id,
            token=token.token,
            public_key="",
        )


class WorkbenchService:
    def __init__(self, session: AsyncSession, user: Component) -> None:
        self.session: AsyncSession = session
        self.user: Component = user

    async def project(self, wpt: WorkbenchProjectToken) -> Project:
        """
        :raise:
            NoResultFound when there is no project with the given token.
        """
        pr: ProjectRepository = ProjectRepository(self.session)

        project = await pr.get_by_token(token=wpt.token)

        LOGGER.info(f"user_id={self.user.component_id}: loaded project with project_id={project.project_id}")

        return project

    async def get_client_list(self, wpt: WorkbenchProjectToken) -> WorkbenchClientList:
        cr: ComponentRepository = ComponentRepository(self.session)
        pr: ProjectRepository = ProjectRepository(self.session)

        client_ids = await pr.list_client_ids(wpt.token)

        clients = await cr.list_clients_by_ids(client_ids)

        client_details = [ClientDetails(**c.dict()) for c in clients]

        LOGGER.info(
            f"user_id={self.user.component_id}: found {len(client_details)} datasource(s) with token={wpt.token}"
        )

        return WorkbenchClientList(clients=client_details)

    async def get_datasource_list(self, wpt: WorkbenchProjectToken) -> WorkbenchDataSourceIdList:
        dsr: DataSourceRepository = DataSourceRepository(self.session)
        pr: ProjectRepository = ProjectRepository(self.session)

        datasource_ids = await pr.list_datasources_ids(wpt.token)

        datasources = [await dsr.load(ds_id) for ds_id in datasource_ids]

        LOGGER.info(f"user_id={self.user.component_id}: found {len(datasources)} datasource(s) with token={wpt.token}")
        return WorkbenchDataSourceIdList(datasources=datasources)

    async def submit_artifact(self, artifact: Artifact) -> ArtifactStatus:
        """
        :raise:
            ValueError if the artifact already exists.
        """
        jms: JobManagementService = job_manager(self.session)

        status = await jms.submit_artifact(artifact)

        LOGGER.info(f"user_id={self.user.component_id}: submitted artifact got artifact_id={status.artifact_id}")

        return status

    async def get_status_artifact(self, artifact: WorkbenchArtifact) -> ArtifactStatus:
        """
        :raise:
            NoResultFound if the artifact was not found in the database.
        """
        ar: ArtifactRepository = ArtifactRepository(self.session)

        status = await ar.get_status(artifact.artifact_id)

        LOGGER.info(f"user_id={self.user.component_id}: got status of artifact_id={artifact.artifact_id}")

        return status

    async def get_artifact(self, artifact: WorkbenchArtifact) -> Artifact:
        """
        :raise:
            ValueError if the requested artifact cannot be found.
        """
        jms: JobManagementService = job_manager(self.session)

        art = await jms.get_artifact(artifact.artifact_id)

        LOGGER.info(f"user_id={self.user.component_id}: downloaded artifact with artifact_id={artifact.artifact_id}")

        return art

    async def get_result(self, artifact: WorkbenchArtifact) -> Result:
        """
        :raise:
            ValueError when the requested result exists on the database but not on disk.

            NoResultFound when there are no results.

            MultipleResultsFound when the result is not unique (database error).
        """
        rr: ResultRepository = ResultRepository(self.session)

        artifact_id = artifact.artifact_id

        result: Result = await rr.get_aggregated_result(artifact_id)

        if not os.path.exists(result.path):
            raise ValueError(f"result_id={result.result_id} not found at path={result.path}")

        LOGGER.info(f"user_id={self.user.component_id}: downloaded results for artifact_id={artifact.artifact_id}")

        return result

    async def get_partial_result(self, artifact_id: str, builder_user_id: str) -> Result:
        """
        :raise:
            ValueError when the requested partial result exists on the database but not on disk.

            NoResultFound when there are no results.

            MultipleResultsFound when the result is not unique (database error).
        """

        rr: ResultRepository = ResultRepository(self.session)

        result: Result = await rr.get_partial_result(artifact_id, builder_user_id)

        if not os.path.exists(result.path):
            raise ValueError(f"partial result_id={result.result_id} not found at path={result.path}")

        LOGGER.info(
            f"user_id={self.user.component_id}: downloaded partial result for artifact_id={artifact_id} and builder_user_id={builder_user_id}"
        )

        return result
