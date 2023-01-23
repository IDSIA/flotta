from ferdelance.config import LOGGING_CONFIG, conf as config

from celery import Celery
from celery.signals import worker_ready, worker_init, worker_shutdown, celeryd_init, after_setup_logger

import logging
import os


LOGGER = logging.getLogger(__name__)


worker = Celery(
    "ferdelance",
    backend=os.getenv("CELERY_BACKEND_URL", None),
    broker=os.getenv("CELERY_BROKER_URL", None),
    include=["ferdelance.worker.tasks"],
)

worker.conf.update(
    result_expires=3600,
)


@after_setup_logger.connect
def setup_loggers(logger, *args, **kwargs):
    import logging.config

    logging.config.dictConfig(LOGGING_CONFIG)


@celeryd_init.connect
def celery_init(sender=None, conf=None, instance=None, **kwargs):
    LOGGER.info("celery daemon initialization")


@worker_init.connect
def config_worker_init(sender=None, conf=None, instance=None, **kwargs):
    LOGGER.info("worker initialization start")


@worker_ready.connect
def config_worker_ready(sender=None, conf=None, instance=None, **kwargs):
    server_url = config.server_url()

    LOGGER.info(f"configured server: {server_url}")
    LOGGER.info("worker ready to accept tasks")


@worker_shutdown.connect
def config_worker_shutdown(sender=None, conf=None, instance=None, **kwargs):
    LOGGER.info("worker shutdown completed")


if __name__ == "__main__":
    worker.start()
