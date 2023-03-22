from ferdelance.config import LOGGING_CONFIG, conf as config

from celery import Celery
from celery.signals import worker_ready, worker_init, worker_shutdown, celeryd_init, after_setup_logger

import logging


LOGGER = logging.getLogger(__name__)


worker = Celery("ferdelance")

worker.config_from_object("ferdelance.worker.celeryconfig")

worker.autodiscover_tasks(["ferdelance.worker.tasks"])

# TODO: add security to workers?


@after_setup_logger.connect
def setup_loggers(logger, *args, **kwargs):
    import logging.config

    logging.config.dictConfig(LOGGING_CONFIG)


@celeryd_init.connect
def celery_init(sender=None, conf=None, instance=None, **kwargs):
    LOGGER.info("celery daemon initialization")
    LOGGER.info(f"using server: {config.server_url()}")


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
