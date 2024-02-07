from typing import Any

from ferdelance.commons import storage_job
from ferdelance.config.config import DataSourceConfiguration, DataSourceStorage, config_manager
from ferdelance.core.artifacts import Artifact
from ferdelance.core.estimators import MeanEstimator
from ferdelance.database.repositories import AsyncSession, ComponentRepository, JobRepository
from ferdelance.node.api import api
from ferdelance.node.services.jobs import JobManagementService
from ferdelance.schemas.components import Component
from ferdelance.schemas.project import Project
from ferdelance.schemas.workbench import WorkbenchProjectToken
from ferdelance.security.exchange import Exchange
from ferdelance.shared.status import JobStatus
from ferdelance.tasks.services import RouteService, TaskExecutionService
from ferdelance.tasks.tasks import TaskError

from tests.utils import TEST_PROJECT_TOKEN, create_node, create_workbench, send_metadata

from fastapi.testclient import TestClient

from pathlib import Path

import pandas as pd
import httpx
import logging
import json
import os
import pickle
import pytest

LOGGER = logging.getLogger(__name__)


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


class SchedulerNode(SimpleNode):
    def __init__(self, session: AsyncSession, component: Component, url: str, private_key_path: Path) -> None:
        super().__init__(Exchange(component.id, private_key_path=private_key_path))

        self.component: Component = component
        self.url: str = url
        self.jms: JobManagementService = JobManagementService(session, self.component)

        self.exc.set_remote_key(self.id(), self.public_key())

    async def done(self, artifact_id: str, job_id: str) -> None:
        await self.jms.task_completed(job_id)
        await self.jms.check(artifact_id)


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

    async def submit(self, scheduler: SchedulerNode, artifact: Artifact) -> Artifact:
        status = await scheduler.jms.submit_artifact(artifact)
        await scheduler.jms.check(status.id)

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

    def post_done(self, artifact_id: str, job_id: str) -> None:
        LOGGER.info("Submitting done!")

    def post_error(self, job_id: str, error: TaskError) -> None:
        LOGGER.error(error.message)
        LOGGER.error(error.stack_trace)
        assert False


def check_files_exists(work_directory: Path, artifact_id: str, job_id: str, iteration: int = 0):
    base_path: Path = storage_job(artifact_id, job_id, iteration, work_directory)

    assert os.path.exists(base_path / "job.json")
    assert os.path.exists(base_path / "task.json")
    with open(base_path / "task.json", "r") as f:
        task_json = json.load(f)
        assert os.path.exists(base_path / f'{task_json["produced_resource_id"]}.pkl')
        assert not os.path.exists(base_path / f'{task_json["produced_resource_id"]}.pkl.enc')


@pytest.mark.asyncio
async def test_execution(session: AsyncSession):
    cr: ComponentRepository = ComponentRepository(session)
    jr: JobRepository = JobRepository(session)

    DATA_PATH_1 = Path("tests") / "integration" / "data" / "california_housing.MedInc1.csv"
    DATA_PATH_2 = Path("tests") / "integration" / "data" / "california_housing.MedInc2.csv"

    BASE_WORK_DIR = Path("tests") / "storage"
    SCHEDULER_WORK_DIR = BASE_WORK_DIR / "artifacts"
    NODE_1_WORK_DIR = BASE_WORK_DIR / "node_1"
    NODE_2_WORK_DIR = BASE_WORK_DIR / "node_2"

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
        scheduler_component = await cr.get_self_component()

        private_key_path: Path = config_manager.get().private_key_location()
        scheduler: SchedulerNode = SchedulerNode(
            session,
            scheduler_component,
            str(server.base_url),
            private_key_path,
        )

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

        # print ids
        print("scheduler", scheduler.id())
        print("client_1 ", client_1.id())
        print("client_2 ", client_2.id())
        print("workbench", workbench.id())

        # setup artifact and jobs
        artifact = Artifact(
            project_id=project.id,
            steps=MeanEstimator().get_steps(),
        )

        artifact = await workbench.submit(scheduler, artifact)

        # checks available tasks
        jobs = await jr.list_jobs_by_artifact_id(artifact.id)

        assert len(jobs) == 4

        # get first task: preparation -----------------------------------------
        next_jobs = await jr.list_scheduled_jobs_for_artifact(artifact.id)

        assert len(next_jobs) == 1
        job = next_jobs[0]

        assert job.component_id == scheduler.id()
        assert job.status == JobStatus.SCHEDULED
        worker = scheduler

        executor = TaskExecutionService(
            TestRouteService(worker.id(), worker.private_key(), server),
            worker.id(),
            artifact.id,
            job.id,
            scheduler.id(),
            scheduler.url,
            worker.remote_key(),
            list(),
            SCHEDULER_WORK_DIR,
        )

        executor.run()

        job = await jr.get_by_id(job.id)
        assert job.status == JobStatus.RUNNING

        await scheduler.done(artifact.id, job.id)

        # test files
        base_path: Path = storage_job(artifact.id, job.id, 0, SCHEDULER_WORK_DIR)

        assert os.path.exists(base_path / "job.json")
        assert os.path.exists(base_path / "task.json")
        with open(base_path / "task.json", "r") as f:
            task_json = json.load(f)
            assert os.path.exists(base_path / f'{task_json["produced_resource_id"]}.pkl')
            assert not os.path.exists(base_path / f'{task_json["produced_resource_id"]}.pkl.enc')

        # get second task: client 1 execution ---------------------------------
        next_jobs = await jr.list_scheduled_jobs_for_artifact(artifact.id)

        assert len(next_jobs) == 1
        job = next_jobs[0]
        assert job.status == JobStatus.SCHEDULED

        worker = clients[job.component_id]

        executor = TaskExecutionService(
            TestRouteService(worker.id(), worker.private_key(), server),
            worker.id(),
            artifact.id,
            job.id,
            scheduler.id(),
            scheduler.url,
            worker.remote_key(),
            worker.datasources(),
            NODE_1_WORK_DIR,
        )

        executor.run()

        job = await jr.get_by_id(job.id)
        assert job.status == JobStatus.RUNNING

        await scheduler.done(artifact.id, job.id)

        # test files
        base_path: Path = storage_job(artifact.id, job.id, 0, NODE_1_WORK_DIR)

        assert os.path.exists(base_path / "task.json")
        with open(base_path / "task.json", "r") as f:
            task_json = json.load(f)

        assert os.path.exists(base_path / f'{task_json["produced_resource_id"]}.pkl')
        assert not os.path.exists(base_path / f'{task_json["produced_resource_id"]}.pkl.enc')

        base_path: Path = storage_job(artifact.id, job.id, 0, SCHEDULER_WORK_DIR)

        assert os.path.exists(base_path / "job.json")
        assert os.path.exists(base_path / f'{task_json["produced_resource_id"]}.pkl')

        # get third task: client 2 execution ----------------------------------
        next_jobs = await jr.list_scheduled_jobs_for_artifact(artifact.id)

        assert len(next_jobs) == 1
        job = next_jobs[0]
        assert job.status == JobStatus.SCHEDULED

        worker = clients[job.component_id]

        executor = TaskExecutionService(
            TestRouteService(worker.id(), worker.private_key(), server),
            worker.id(),
            artifact.id,
            job.id,
            scheduler.id(),
            scheduler.url,
            worker.remote_key(),
            worker.datasources(),
            NODE_2_WORK_DIR,
        )

        executor.run()

        job = await jr.get_by_id(job.id)
        assert job.status == JobStatus.RUNNING

        await scheduler.done(artifact.id, job.id)

        # test files
        base_path: Path = storage_job(artifact.id, job.id, 0, NODE_2_WORK_DIR)

        assert os.path.exists(base_path / "task.json")
        with open(base_path / "task.json", "r") as f:
            task_json = json.load(f)

        assert os.path.exists(base_path / f'{task_json["produced_resource_id"]}.pkl')
        assert not os.path.exists(base_path / f'{task_json["produced_resource_id"]}.pkl.enc')

        base_path: Path = storage_job(artifact.id, job.id, 0, SCHEDULER_WORK_DIR)

        assert os.path.exists(base_path / "job.json")
        assert os.path.exists(base_path / f'{task_json["produced_resource_id"]}.pkl')

        # get last task: scheduler completion ---------------------------------
        next_jobs = await jr.list_scheduled_jobs_for_artifact(artifact.id)

        assert len(next_jobs) == 1
        job = next_jobs[0]
        assert job.status == JobStatus.SCHEDULED

        assert job.component_id == scheduler.id()
        worker = scheduler

        executor = TaskExecutionService(
            TestRouteService(worker.id(), worker.private_key(), server),
            worker.id(),
            artifact.id,
            job.id,
            scheduler.id(),
            scheduler.url,
            worker.remote_key(),
            list(),
            SCHEDULER_WORK_DIR,
        )

        executor.run()

        job = await jr.get_by_id(job.id)
        assert job.status == JobStatus.RUNNING

        await scheduler.done(artifact.id, job.id)

        next_jobs = await jr.list_scheduled_jobs_for_artifact(artifact.id)
        assert len(next_jobs) == 0

        # test files
        base_path: Path = storage_job(artifact.id, job.id, 0, SCHEDULER_WORK_DIR)

        assert os.path.exists(base_path / "job.json")
        assert os.path.exists(base_path / "task.json")
        with open(base_path / "task.json", "r") as f:
            task_json = json.load(f)

        assert os.path.exists(base_path / f'{task_json["produced_resource_id"]}.pkl')
        assert not os.path.exists(base_path / f'{task_json["produced_resource_id"]}.pkl.enc')

        # check result

        with open(base_path / f'{task_json["produced_resource_id"]}.pkl', "rb") as f:
            result = pickle.load(f)

        assert isinstance(result, dict)

        mean_df = result["mean"]

        assert isinstance(mean_df, pd.Series)

        data_df = pd.concat([pd.read_csv(DATA_PATH_1), pd.read_csv(DATA_PATH_2)], axis=0)
        expected_df = data_df.mean()

        assert type(mean_df) == type(expected_df)

        assert all(mean_df.index == expected_df.index)

        for i in mean_df.index:
            assert abs(mean_df[i] - expected_df[i]) < 1e6
