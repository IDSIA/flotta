from typing import Any

from ferdelance.jobs_backend import TaskArguments
from ferdelance.schemas.errors import ClientTaskError
from ferdelance.worker.celery import worker

from celery import Task

import logging
import requests
import traceback

from ferdelance.worker.jobs.job import JobService


class EstimationTask(Task):
    abstract = True

    def __init__(self) -> None:
        super().__init__()
        self.job_service: JobService
        self.artifact_id: str
        self.job_id: str

    def __call__(self, *args, **kwargs):
        return self.run(*args, **kwargs)

    def on_failure(self, exc, task_id, args, kwargs, einfo):
        logging.critical(f"{task_id} failed: exc={exc!r}")
        logging.critical(f"{task_id} failed: args={args!r}")
        logging.critical(f"{task_id} failed: kwargs={kwargs!r}")
        logging.critical(f"{task_id} failed: extra_info={einfo!r}")

    def setup(self, args: TaskArguments) -> None:
        self.job_service = JobService(args)
        self.artifact_id = args.artifact_id
        self.job_id = args.job_id

    def execute(self):
        try:
            self.job_service.run_estimation()

        except requests.HTTPError as e:
            logging.error(f"artifact_id={self.job_id}: {e}")
            logging.exception(e)
            raise e


@worker.task(
    ignore_result=True,
    bind=True,
    base=EstimationTask,
)
def estimate(self: EstimationTask, raw_args: dict[str, Any]) -> None:
    task_id: str = str(self.request.id)
    try:
        logging.info(f"worker: beginning execution task={task_id}")

        args = TaskArguments(**raw_args)

        self.setup(args)
        self.execute()

    except Exception as e:
        logging.error(f"task_id={task_id}: job_id={self.job_id}: {e}")
        logging.exception(e)

        error = ClientTaskError(
            job_id=self.job_id,
            message=str(e),
            stack_trace="".join(traceback.TracebackException.from_exception(e).format()),
        )

        self.job_service.routes_service.post_error(self.job_id, self.artifact_id, error)
