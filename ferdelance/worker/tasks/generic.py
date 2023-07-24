from celery import Task
import logging

from ferdelance.schemas.errors import TaskError
from ferdelance.worker.jobs.services import JobService


class GenericTask(Task):
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

    def error(self, error: TaskError):
        logging.error(f"job_id={error.job_id}: {error.message}")
        self.job_service.routes_service.post_error(self.job_id, self.artifact_id, error)
