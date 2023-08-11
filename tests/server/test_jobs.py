from ferdelance.config import get_logger
from ferdelance.database.data import TYPE_CLIENT
from ferdelance.database.repositories import JobRepository
from ferdelance.database.tables import Artifact, Component
from ferdelance.shared.status import JobStatus

from sqlalchemy.ext.asyncio import AsyncSession

import pytest

LOGGER = get_logger(__name__)


@pytest.mark.asyncio
async def test_jobs_next(session: AsyncSession):
    artifact_id_1: str = "artifact1"
    artifact_id_2: str = "artifact2"
    client_id_1: str = "client1"
    client_id_2: str = "client2"

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
            version="test",
            public_key="1",
            machine_system="1",
            machine_mac_address="1",
            machine_node="1",
            ip_address="1",
            type_name=TYPE_CLIENT,
        )
    )
    session.add(
        Component(
            id=client_id_2,
            version="test",
            public_key="2",
            machine_system="2",
            machine_mac_address="2",
            machine_node="2",
            ip_address="2",
            type_name=TYPE_CLIENT,
        )
    )

    await session.commit()

    jr: JobRepository = JobRepository(session)

    sc_1 = await jr.schedule_job(artifact_id_1, client_id_1)
    sc_2 = await jr.schedule_job(artifact_id_1, client_id_2)
    sc_3 = await jr.schedule_job(artifact_id_2, client_id_1)

    job1 = await jr.next_job_for_component(client_id_1)

    assert job1 is not None
    assert job1.execution_time is None
    assert job1.artifact_id == artifact_id_1
    assert JobStatus[job1.status] == JobStatus.SCHEDULED

    await jr.start_execution(job1)

    sc_1 = await jr.get(sc_1)
    sc_2 = await jr.get(sc_2)
    sc_3 = await jr.get(sc_3)
    job1 = await jr.get(job1)

    assert JobStatus[job1.status] == JobStatus.RUNNING
    assert job1.execution_time is not None
    assert sc_2.execution_time is None
    assert sc_3.execution_time is None

    await jr.mark_completed(job1.id, job1.component_id)

    sc_1 = await jr.get(sc_1)
    sc_2 = await jr.get(sc_2)
    sc_3 = await jr.get(sc_3)
    job1 = await jr.get(job1)

    assert JobStatus[job1.status] == JobStatus.COMPLETED
    assert job1.termination_time is not None
    assert sc_2.termination_time is None
    assert sc_3.termination_time is None

    job2 = await jr.next_job_for_component(client_id_1)

    assert job2 is not None
    assert job1.id != job2.id
