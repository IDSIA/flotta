from typing import Any

from ferdelance.database import SessionLocal

from ..celery import worker
from ...database.services import ClientTaskService, DataSourceService

from ferdelance_shared.schemas import Artifact


@worker.task(
    ignore_result=False,
    bind=True,
)
def schedule(self, raw_artifact: dict[str, Any]):

    task_id = str(self.request.id)
    artifact = Artifact(**raw_artifact)

    with SessionLocal() as db:

        cts: ClientTaskService = ClientTaskService(db)
        dss: DataSourceService = DataSourceService(db)

        # TODO this is there temporally. It will be done inside a celery worker...
        task_db = cts.create_task(artifact)

        client_ids = set()
        for query in artifact.queries:
            client_id: str = dss.get_client_id_by_datasource_id(query.datasources_id)
            client_ids.update(client_id)

        for client_id in list(client_ids):
            cts.create_client_task(artifact_db.artifact_id, client_id, task_db.task_id)

        # TODO: ...until there
