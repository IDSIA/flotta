from ferdelance.config import conf
from ferdelance.client.services.actions import ActionService
from ferdelance.schemas.errors import WorkerJobError
from ferdelance.worker.celery import worker

from celery import Task

import json
import logging
import requests
import traceback


class ExecutionRouter:
    def __init__(self, token: str) -> None:
        self.server = conf.server_url()
        self.token = token

    def headers(self):
        return {"Authorization": f"Bearer {self.token}"}

    def post_error(self, job_id: str, error: WorkerJobError) -> None:
        logging.info(f"job_id={job_id}: posting error")

        res = requests.post(
            f"{self.server}/worker/error/",
            headers=self.headers(),
            data=json.dumps(error.dict()),
        )

        res.raise_for_status()

    def get_task(self, job_id: str):
        res = requests.get(f"{self.server}/worker")


class ExecutionTask(Task):
    abstract = True

    def __init__(self) -> None:
        super().__init__()

        self.router: ExecutionRouter = None  # type: ignore

    def __call__(self, *args, **kwargs):
        return self.run(*args, **kwargs)

    def on_failure(self, exc, task_id, args, kwargs, einfo):
        logging.critical(f"{task_id} failed: exc={exc!r}")
        logging.critical(f"{task_id} failed: args={args!r}")
        logging.critical(f"{task_id} failed: kwargs={kwargs!r}")
        logging.critical(f"{task_id} failed: extra_info={einfo!r}")

    def setup(self, token: str) -> None:
        self.router = ExecutionRouter(token)

    def execute(self, job_id: str):
        try:
            logging.debug(f"using server {conf.server_url()}")

            action_service = ActionService(self.config, self.train_queue, self.estimate_queue)

            # TODO

        except requests.HTTPError as e:
            logging.error(f"artifact_id={job_id}: {e}")
            logging.exception(e)
            raise e


@worker.task(
    ignore_result=True,
    bind=True,
    base=ExecutionTask,
)
def execute(self: ExecutionTask, token: str, job_id: str) -> None:
    task_id: str = str(self.request.id)
    try:
        logging.info(f"worker: beginning execution task={task_id}")

        self.setup(token)
        self.execute(job_id)

    except Exception as e:
        logging.error(f"task_id={task_id}: job_id={job_id}: {e}")
        logging.exception(e)

        error = WorkerJobError(
            job_id=job_id,
            message=str(e),
            stack_trace="".join(traceback.TracebackException.from_exception(e).format()),
        )

        self.router.post_error(job_id, error)
