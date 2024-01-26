from typing import Any

from requests.models import Response as Response

from ferdelance.config.config import DataSourceConfiguration, DataSourceStorage, config_manager
from ferdelance.core.artifacts import Artifact
from ferdelance.core.estimators import MeanEstimator
from ferdelance.database.repositories import AsyncSession, ComponentRepository, JobRepository
from ferdelance.database.repositories.artifact import ArtifactRepository
from ferdelance.node.api import api
from ferdelance.node.services.jobs import JobManagementService
from ferdelance.schemas.project import Project
from ferdelance.schemas.workbench import WorkbenchProjectToken
from ferdelance.security.exchange import Exchange
from ferdelance.tasks.jobs.execution import TaskExecutor
from ferdelance.tasks.services.routes import RouteService

from tests.utils import TEST_PROJECT_TOKEN, create_node, create_workbench, send_metadata

from fastapi.testclient import TestClient

from pathlib import Path

import httpx
import json
import os
import pytest


class SimpleNode:
    def __init__(self, exchange: Exchange) -> None:
        self.exc = exchange

    def private_key(self) -> str:
        return self.exc.transfer_private_key()

    def public_key(self) -> str:
        return self.exc.transfer_public_key()

    def remote_key(self) -> str:
        return self.exc.transfer_remote_key()

    def id(self) -> str:
        assert self.exc.source_id
        return self.exc.source_id

    def remote_id(self) -> str:
        assert self.exc.target_id
        return self.exc.target_id


class ClientNode(SimpleNode):
    def __init__(self, api: TestClient, data: DataSourceStorage) -> None:
        super().__init__(create_node(api))
        self.data: DataSourceStorage = data

        send_metadata(api, self.exc, self.data.metadata())

    def datasources(self) -> list[dict[str, Any]]:
        return [ds.dict() for ds in self.data.ds_configs]


class WorkBenchNode(SimpleNode):
    def __init__(self, api: TestClient, public_key: str) -> None:
        super().__init__(create_workbench(api, public_key))

        self.api: TestClient = api

    def project(self, token: str = TEST_PROJECT_TOKEN) -> Project:
        wpt = WorkbenchProjectToken(token=token)

        headers, payload = self.exc.create(wpt.json())

        res = self.api.request(
            "GET",
            "/workbench/project",
            headers=headers,
            content=payload,
        )

        res.raise_for_status()

        _, res_payload = self.exc.get_payload(res.content)
        return Project(**json.loads(res_payload))

    async def submit(self, session: AsyncSession, artifact: Artifact) -> Artifact:
        cr = ComponentRepository(session)
        component = await cr.get_by_id(self.remote_id())

        jms = JobManagementService(session, component)
        status = await jms.submit_artifact(artifact)

        artifact.id = status.id

        return artifact


class TestRouteService(RouteService):
    def __init__(self, component_id: str, private_key: str, api: TestClient) -> None:
        super().__init__(component_id, private_key)

        self.api = api

    def _stream_get(self, url: str, headers: dict[str, str], data: Any = None):
        return self.api.stream(
            "GET",
            url,
            headers=headers,
            content=data,
        )

    def _get(self, url: str, headers: dict[str, str], data: Any = None) -> httpx.Response:
        return self.api.request(
            "GET",
            url,
            headers=headers,
            content=data,
        )

    def _post(self, url: str, headers: dict[str, str], data: Any = None) -> httpx.Response:
        return self.api.post(
            url,
            headers=headers,
            content=data,
        )


@pytest.mark.asyncio
async def test_execution(session: AsyncSession):
    cr: ComponentRepository = ComponentRepository(session)
    ar: ArtifactRepository = ArtifactRepository(session)
    jr: JobRepository = JobRepository(session)

    DATA_PATH_1 = Path("tests") / "integration" / "data" / "california_housing.MedInc1.csv"
    DATA_PATH_2 = Path("tests") / "integration" / "data" / "california_housing.MedInc2.csv"

    assert os.path.exists(DATA_PATH_1)
    assert os.path.exists(DATA_PATH_2)

    data_1 = DataSourceStorage(
        [
            DataSourceConfiguration(
                name="california1",
                token=[TEST_PROJECT_TOKEN],
                kind="file",
                type="csv",
                path=str(DATA_PATH_1),
            )
        ],
    )
    data_2 = DataSourceStorage(
        [
            DataSourceConfiguration(
                name="california2",
                kind="file",
                type="csv",
                path=str(DATA_PATH_2),
                token=[TEST_PROJECT_TOKEN],
            )
        ],
    )

    with TestClient(api) as server:
        # setup scheduler
        sc_component = await cr.get_self_component()
        private_key_path: Path = config_manager.get().private_key_location()

        scheduler_url = str(server.base_url)
        scheduler_id = sc_component.id

        scheduler: SimpleNode = SimpleNode(Exchange(sc_component.id, private_key_path=private_key_path))
        scheduler.exc.set_remote_key(scheduler_id, scheduler.public_key())

        # setup clients
        client_1: ClientNode = ClientNode(server, data_1)
        client_2: ClientNode = ClientNode(server, data_2)

        clients: dict[str, ClientNode] = {
            client_1.id(): client_1,
            client_2.id(): client_2,
        }

        # setup workbench
        workbench: WorkBenchNode = WorkBenchNode(server, scheduler.public_key())

        project = workbench.project(TEST_PROJECT_TOKEN)

        # setup artifact and jobs
        artifact = Artifact(
            project_id=project.id,
            steps=MeanEstimator().get_steps(),
        )

        artifact = await workbench.submit(session, artifact)

        # checks available tasks
        jobs = await jr.list_jobs_by_artifact_id(artifact.id)

        assert len(jobs) == 4

        # get first task: preparation
        jobs = await jr.list_unlocked_jobs_by_artifact_id(artifact.id)

        assert len(jobs) == 1
        job = jobs[0]

        assert job.component_id == scheduler.id()
        worker = scheduler

        executor = TaskExecutor(
            TestRouteService(worker.id(), worker.private_key(), server),
            worker.id(),
            artifact.id,
            job.id,
            scheduler_id,
            scheduler_url,
            worker.remote_key(),
            list(),
        )

        executor.run()

        # get second task: client 1 execution
        jobs = await jr.list_unlocked_jobs_by_artifact_id(artifact.id)

        assert len(jobs) == 1
        job = jobs[0]

        worker = clients[job.component_id]

        executor = TaskExecutor(
            TestRouteService(worker.id(), worker.private_key(), server),
            worker.id(),
            artifact.id,
            job.id,
            scheduler_id,
            scheduler_url,
            worker.remote_key(),
            list(),
        )

        executor.run()

        # get third task: client 2 execution
        jobs = await jr.list_unlocked_jobs_by_artifact_id(artifact.id)

        assert len(jobs) == 1
        job = jobs[0]

        worker = clients[job.component_id]

        executor = TaskExecutor(
            TestRouteService(worker.id(), worker.private_key(), server),
            worker.id(),
            artifact.id,
            job.id,
            scheduler_id,
            scheduler_url,
            worker.remote_key(),
            list(),
        )

        executor.run()

        # get last task: scheduler completion
        jobs = await jr.list_unlocked_jobs_by_artifact_id(artifact.id)

        assert len(jobs) == 1
        job = jobs[0]

        assert job.component_id == scheduler.id
        worker = scheduler

        executor = TaskExecutor(
            TestRouteService(worker.id(), worker.private_key(), server),
            worker.id(),
            artifact.id,
            job.id,
            scheduler_id,
            scheduler_url,
            worker.private_key(),
            list(),
        )

        executor.run()
