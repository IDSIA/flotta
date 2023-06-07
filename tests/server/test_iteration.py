from ferdelance.config import conf
from ferdelance.database.data import TYPE_WORKER
from ferdelance.database.repositories import ComponentRepository
from ferdelance.workbench.interface import Artifact
from ferdelance.schemas.models import Model
from ferdelance.schemas.updates import UpdateExecute
from ferdelance.schemas.plans import TrainTestSplit, IterativePlan
from ferdelance.server.services import ClientService, WorkerService, WorkbenchService
from ferdelance.server.startup import ServerStartup
from ferdelance.jobs import JobManagementService

from tests.utils import (
    TEST_PROJECT_TOKEN,
    get_metadata,
)

from sqlalchemy.ext.asyncio import AsyncSession

import logging
import os
import pytest
import shutil
import logging

LOGGER = logging.getLogger(__name__)


@pytest.mark.asyncio
async def test_iteration(session: AsyncSession):
    await ServerStartup(session).startup()

    cr: ComponentRepository = ComponentRepository(session)

    client_component, _ = await cr.create_client("client-1", "1", "2", "3", "4", "5", "6")
    worker_component, _ = await cr.create_component(TYPE_WORKER, "worker-1")

    client_service = ClientService(session, client_component.client_id)
    worker_service = WorkerService(session, worker_component.component_id)
    workbench_service = WorkbenchService(session, "workbench-1")

    jms = JobManagementService(session)

    # this add the project
    metadata = get_metadata()
    await client_service.update_metadata(metadata)

    # this get the project
    project = await workbench_service.project(TEST_PROJECT_TOKEN)
    label: str = project.data.features[0].name

    # creates an artifact
    artifact = Artifact(
        project_id=project.project_id,
        transform=project.extract(),
        model=Model(name="model", strategy=""),
        plan=IterativePlan(
            iterations=2,
            local_plan=TrainTestSplit(
                label=label,
                test_percentage=0.5,
            ),
        ).build(),
    )

    # this submits the artifact
    status = await workbench_service.submit_artifact(artifact)
    artifact_id = status.artifact_id

    assert artifact_id is not None

    # client asks for work to do
    next_action = await client_service.update({})

    assert isinstance(next_action, UpdateExecute)

    # client get task to do
    task = await client_service.get_task(next_action)

    """...simulate client work..."""

    # client creates result
    result = await client_service.result(task.job_id)

    # server checks for aggregation

    can_aggregate = await jms.check_for_aggregation(result)

    assert can_aggregate

    await jms.schedule_aggregation_job(result, worker_component.component_id)

    # worker ask for aggregation task

    await worker_service.get_task(result.job_id)

    """...simulate worker aggregation..."""

    await worker_service.completed(result.job_id)

    shutil.rmtree(os.path.join(conf.STORAGE_ARTIFACTS, artifact_id))
