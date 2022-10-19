from ipaddress import ip_address
from ferdelance.database import SessionLocal
from ferdelance.database.services import JobService
from ferdelance.database.tables import Job, Client, Artifact

from ferdelance_shared.status import JobStatus

from .utils import (
    setup_test_client,
    setup_test_database,
    setup_rsa_keys,
    teardown_test_database,
    bytes_from_public_key,
)

from datetime import datetime
from uuid import uuid4

import logging
import random

LOGGER = logging.getLogger(__name__)


class TestJobsClass:

    def setup_class(self):
        """Class setup. This will be executed once each test. The setup will:
        - Create the client.
        - Create a new database on the remote server specified by `DB_HOST`, `DB_USER`, and `DB_PASS` (all env variables.).
            The name of the database is randomly generated using UUID4, if not supplied via `DB_SCHEMA` env variable.
            The database will be used as the server's database.
        - Populate this database with the required tables.
        - Generate and save to the database the servers' keys using the hardcoded `SERVER_MAIN_PASSWORD`.
        - Generate the local public/private keys to simulate a client application.
        """
        LOGGER.info('setting up')

        self.client = setup_test_client()

        self.db_string, self.db_string_no_db = setup_test_database()

        self.private_key = setup_rsa_keys()
        self.public_key = self.private_key.public_key()
        self.public_key_bytes = bytes_from_public_key(self.public_key)

        random.seed(42)

        self.server_key = None
        self.token = None

        LOGGER.info('setup completed')

    def teardown_class(self):
        """Class teardown. This method will ensure that the database is closed and deleted from the remote dbms.
        Note that all database connections still open will be forced to close by this method.
        """
        LOGGER.info('tearing down')

        teardown_test_database(self.db_string_no_db)

        LOGGER.info('teardown completed')

    def test_next_job(self):
        artifact_id_1: str = 'artifact1'
        artifact_id_2: str = 'artifact2'
        client_id_1: str = 'client1'
        client_id_2: str = 'client2'

        db = SessionLocal()

        db.add(Artifact(artifact_id=artifact_id_1, path='.', status='',))
        db.add(Artifact(artifact_id=artifact_id_2, path='.', status='',))

        db.add(Client(client_id=client_id_1, version='test', public_key='1', machine_system='1', machine_mac_address='1', machine_node='1', ip_address='1', type='CLIENT',))
        db.add(Client(client_id=client_id_2, version='test', public_key='2', machine_system='2', machine_mac_address='2', machine_node='2', ip_address='2', type='CLIENT',))
        db.commit()

        js: JobService = JobService(db)

        sc_1 = js.schedule_job(artifact_id_1, client_id_1)
        sc_2 = js.schedule_job(artifact_id_1, client_id_2)
        sc_3 = js.schedule_job(artifact_id_2, client_id_1)

        job1 = js.next_job_for_client(client_id_1)

        assert job1 is not None
        assert job1.execution_time is None
        assert job1.artifact_id == artifact_id_1
        assert JobStatus[job1.status] == JobStatus.SCHEDULED

        js.start_execution(job1)

        db.refresh(sc_1)
        db.refresh(sc_2)
        db.refresh(sc_3)
        db.refresh(job1)

        assert JobStatus[job1.status] == JobStatus.RUNNING
        assert job1.execution_time is not None
        assert sc_2.execution_time is None
        assert sc_3.execution_time is None

        js.stop_execution(job1.artifact_id, job1.client_id)

        db.refresh(sc_1)
        db.refresh(sc_2)
        db.refresh(sc_3)
        db.refresh(job1)

        assert JobStatus[job1.status] == JobStatus.COMPLETED
        assert job1.termination_time is not None
        assert sc_2.termination_time is None
        assert sc_3.termination_time is None

        job2 = js.next_job_for_client(client_id_1)

        assert job2 is not None
        assert job1.job_id != job2.job_id

        db.close()
