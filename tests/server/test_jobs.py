from ferdelance.database import DataBase, Base
from ferdelance.database.tables import *
from ferdelance.database.services import JobService
from ferdelance.database.tables import Client, Artifact
from ferdelance.shared.status import JobStatus
from ferdelance.shared.exchange import Exchange

from sqlalchemy.ext.asyncio import AsyncSession

import logging
import pytest

LOGGER = logging.getLogger(__name__)


class TestJobsClass:

    @pytest.mark.asyncio
    async def test_jobs_next(self):
        artifact_id_1: str = 'artifact1'
        artifact_id_2: str = 'artifact2'
        client_id_1: str = 'client1'
        client_id_2: str = 'client2'

        inst = DataBase()

        async with inst.engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)

        async with AsyncSession(inst.engine) as session:
            session.add(Artifact(artifact_id=artifact_id_1, path='.', status='',))
            session.add(Artifact(artifact_id=artifact_id_2, path='.', status='',))

            session.add(Client(client_id=client_id_1, version='test', public_key='1', machine_system='1',
                        machine_mac_address='1', machine_node='1', ip_address='1', type='CLIENT',))
            session.add(Client(client_id=client_id_2, version='test', public_key='2', machine_system='2',
                        machine_mac_address='2', machine_node='2', ip_address='2', type='CLIENT',))

            await session.commit()

            js: JobService = JobService(session)

            sc_1 = await js.schedule_job(artifact_id_1, client_id_1)
            sc_2 = await js.schedule_job(artifact_id_1, client_id_2)
            sc_3 = await js.schedule_job(artifact_id_2, client_id_1)

            job1 = await js.next_job_for_client(client_id_1)

            assert job1 is not None
            assert job1.execution_time is None
            assert job1.artifact_id == artifact_id_1
            assert JobStatus[job1.status] == JobStatus.SCHEDULED

            await js.start_execution(job1)

            await session.refresh(sc_1)
            await session.refresh(sc_2)
            await session.refresh(sc_3)
            await session.refresh(job1)

            assert JobStatus[job1.status] == JobStatus.RUNNING
            assert job1.execution_time is not None
            assert sc_2.execution_time is None
            assert sc_3.execution_time is None

            await js.stop_execution(job1.artifact_id, job1.client_id)

            await session.refresh(sc_1)
            await session.refresh(sc_2)
            await session.refresh(sc_3)
            await session.refresh(job1)

            assert JobStatus[job1.status] == JobStatus.COMPLETED
            assert job1.termination_time is not None
            assert sc_2.termination_time is None
            assert sc_3.termination_time is None

            job2 = await js.next_job_for_client(client_id_1)

            assert job2 is not None
            assert job1.job_id != job2.job_id
