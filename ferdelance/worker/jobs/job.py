from ferdelance.client.config import DataConfig
from ferdelance.jobs_backend import TaskArguments
from ferdelance.schemas.client import ClientTask, DataSourceConfig
from ferdelance.schemas.errors import ClientTaskError
from ferdelance.schemas.models.metrics import Metrics
from ferdelance.schemas.updates import UpdateExecute
from ferdelance.shared.exchange import Exchange
from ferdelance.worker.jobs.results import ExecutionResult
from ferdelance.worker.jobs.execution import run_estimate, run_training

import logging
import os
import requests

LOGGER = logging.getLogger(__name__)


class LocalRouteService:
    def __init__(self, args: TaskArguments) -> None:
        self.exc: Exchange = Exchange()
        self.exc.load_key(args.private_key_location)
        self.exc.set_remote_key(args.server_public_key)
        self.exc.set_token(args.token)

        self.server: str = args.server_url

    def get_task(self, task: UpdateExecute) -> ClientTask:
        LOGGER.info("requesting new client task")

        res = requests.get(
            f"{self.server}/client/task",
            headers=self.exc.headers(),
            data=self.exc.create_payload(task.dict()),
        )

        res.raise_for_status()

        return ClientTask(**self.exc.get_payload(res.content))

    def post_result(self, job_id: str, path_in: str):
        path_out = f"{path_in}.enc"

        self.exc.encrypt_file_for_remote(path_in, path_out)

        res = requests.post(
            f"{self.server}/client/result/{job_id}",
            headers=self.exc.headers(),
            data=open(path_out, "rb"),
        )

        if os.path.exists(path_out):
            os.remove(path_out)

        res.raise_for_status()

        LOGGER.info(f"job_id={job_id}: result from source={path_in} upload successful")

    def post_metrics(self, metrics: Metrics):
        res = requests.post(
            f"{self.server}/client/metrics",
            headers=self.exc.headers(),
            data=self.exc.create_payload(metrics.dict()),
        )

        res.raise_for_status()

        LOGGER.info(
            f"job_id={metrics.job_id}: metrics for artifact_id={metrics.artifact_id} from source={metrics.source} upload successful"
        )

    def post_error(self, job_id: str, artifact_id: str, error: ClientTaskError) -> None:
        LOGGER.error(f"job_id={job_id}: error for artifact_id={artifact_id} error: {error}")
        res = requests.post(
            f"{self.server}/client/error/{job_id}",
            headers=self.exc.headers(),
            data=self.exc.create_payload(error.dict()),
        )

        res.raise_for_status()


class JobService:
    def __init__(self, args: TaskArguments) -> None:
        datasources: list[DataSourceConfig] = [DataSourceConfig(**d) for d in args.datasources]

        self.data = DataConfig(args.workdir, datasources)
        self.routes_service: LocalRouteService = LocalRouteService(args)
        self.artifact_id: str = args.artifact_id
        self.job_id: str = args.job_id

    def run(self):
        task: ClientTask = self.routes_service.get_task(
            UpdateExecute(
                action="",
                artifact_id=self.artifact_id,
                job_id=self.job_id,
            )
        )

        if task.artifact.is_estimation():
            self.run_estimation()

        elif task.artifact.is_model():
            res: ExecutionResult = run_training(self.data, task)

            for m in res.metrics:
                m.job_id = res.job_id
                self.routes_service.post_metrics(m)

            self.routes_service.post_result(res.job_id, res.path)
        else:
            self.routes_service.post_error(
                task.job_id,
                task.artifact.id,
                ClientTaskError(
                    job_id=task.job_id,
                    message="Malformed artifact",
                ),
            )

    def run_estimation(self):
        task: ClientTask = self.routes_service.get_task(
            UpdateExecute(
                action="",
                artifact_id=self.artifact_id,
                job_id=self.job_id,
            )
        )

        res: ExecutionResult = run_estimate(self.data, task)

        self.routes_service.post_result(res.job_id, res.path)

    def run_training(self):
        task: ClientTask = self.routes_service.get_task(
            UpdateExecute(
                action="",
                artifact_id=self.artifact_id,
                job_id=self.job_id,
            )
        )

        res: ExecutionResult = run_training(self.data, task)

        for m in res.metrics:
            m.job_id = res.job_id
            self.routes_service.post_metrics(m)

        self.routes_service.post_result(res.job_id, res.path)
