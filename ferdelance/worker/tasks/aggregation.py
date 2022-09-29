from typing import Any

from ..celery import worker


@worker.task(
    ignore_result=False,
    bind=True,
)
def aggregation(self, raw_strategy: dict[str, Any]) -> str:

    return str(self.request.id)
