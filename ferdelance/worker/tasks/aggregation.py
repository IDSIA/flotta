from typing import Any

from ferdelance.database import SessionLocal

from ferdelance_shared.schemas import Artifact

from ..celery import worker

import logging

LOGGER = logging.getLogger(__name__)


@worker.task(
    ignore_result=False,
    bind=True,
)
def aggregation(self, raw_artifact: dict[str, Any]) -> str:

    # task_id = str(self.request.id)

    artifact = Artifact(**raw_artifact)

    with SessionLocal() as db:

        LOGGER.info(f'SCHEDULING AGGREGATION {artifact.artifact_id}')

        # TODO

        raise NotImplementedError()
