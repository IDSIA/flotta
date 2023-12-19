from ferdelance.const import TYPE_CLIENT
from ferdelance.core.interfaces import SchedulerJob
from ferdelance.database.repositories.component import ComponentRepository
from ferdelance.database.repositories import JobRepository
from ferdelance.database.tables import Artifact, Component, Resource
from ferdelance.logging import get_logger
from ferdelance.shared.status import JobStatus

from tests.dummies import DummyStep

from sqlalchemy.ext.asyncio import AsyncSession

import pytest

LOGGER = get_logger(__name__)


@pytest.mark.asyncio
async def test_jobs_next(session: AsyncSession):
    artifact_id_1: str = "artifact1"
    artifact_id_2: str = "artifact2"
    client_id_1: str = "client1"
    client_id_2: str = "client2"

    resource_id_1: str = "resource1"
    resource_id_2: str = "resource2"
    resource_id_3: str = "resource3"

    session.add(
        Artifact(
            id=artifact_id_1,
            path=".",
            status="",
        )
    )
    session.add(
        Artifact(
            id=artifact_id_2,
            path=".",
            status="",
        )
    )

    session.add(
        Component(
            id=client_id_1,
            name="client1",
            version="test",
            public_key="1",
            ip_address="1",
            url="",
            type_name=TYPE_CLIENT,
        )
    )
    session.add(
        Component(
            id=client_id_2,
            name="client2",
            version="test",
            public_key="2",
            ip_address="2",
            url="",
            type_name=TYPE_CLIENT,
        )
    )

    session.add(
        Resource(
            id=resource_id_1,
            path="",
            component_id=client_id_1,
        )
    )
    session.add(
        Resource(
            id=resource_id_2,
            path="",
            component_id=client_id_1,
        )
    )
    session.add(
        Resource(
            id=resource_id_3,
            path="",
            component_id=client_id_1,
        )
    )

    await session.commit()

    cr = ComponentRepository(session)
    c1 = await cr.get_by_id(client_id_1)
    c2 = await cr.get_by_id(client_id_2)

    job_a1_c1 = SchedulerJob(id=0, worker=c1, iteration=0, step=DummyStep(), locks=[1])
    job_a1_c2 = SchedulerJob(id=1, worker=c2, iteration=0, step=DummyStep(), locks=[1])
    job_a2_c1 = SchedulerJob(id=2, worker=c1, iteration=0, step=DummyStep(), locks=[1])

    jr: JobRepository = JobRepository(session)

    sc_1 = await jr.create_job(artifact_id_1, job_a1_c1, resource_id=resource_id_1)
    sc_2 = await jr.create_job(artifact_id_1, job_a1_c2, resource_id=resource_id_2)
    sc_3 = await jr.create_job(artifact_id_2, job_a2_c1, resource_id=resource_id_3)

    await jr.schedule_job(sc_1)

    job1 = await jr.next_job_for_component(client_id_1)

    assert job1 is not None
    assert job1.execution_time is None
    assert job1.artifact_id == artifact_id_1
    assert job1.status == JobStatus.SCHEDULED

    await jr.start_execution(job1)

    sc_1 = await jr.get(sc_1)
    sc_2 = await jr.get(sc_2)
    sc_3 = await jr.get(sc_3)
    job1 = await jr.get(job1)

    assert job1.status == JobStatus.RUNNING
    assert job1.execution_time is not None
    assert sc_2.execution_time is None
    assert sc_3.execution_time is None

    await jr.complete_execution(job1)

    sc_1 = await jr.get(sc_1)
    sc_2 = await jr.get(sc_2)
    sc_3 = await jr.get(sc_3)
    job1 = await jr.get(job1)

    assert job1.status == JobStatus.COMPLETED
    assert job1.termination_time is not None
    assert sc_2.termination_time is None
    assert sc_3.termination_time is None

    await jr.schedule_job(sc_3)

    job2 = await jr.next_job_for_component(client_id_1)

    assert job2 is not None
    assert job1.id != job2.id
