from celery import Celery
from celery.signals import worker_ready, worker_init, worker_shutdown, celeryd_init

import logging
import os

LOGGER = logging.getLogger(__name__)


worker = Celery(
    'ferdelance',
    backend=os.getenv('CELERY_BACKEND_URL', None),
    broker=os.getenv('CELERY_BROKER_URL', None),
    include=['ferdelance.worker.tasks'],
)

worker.conf.update(
    result_expires=3600,
)


@celeryd_init.connect
def celery_init(sender=None, conf=None, instance=None, **kwargs):
    LOGGER.info('celery daemon initialization')


@worker_init.connect
def config_worker_init(sender=None, conf=None, instance=None, **kwargs):
    LOGGER.info('worker initialization start')


@worker_ready.connect
def config_worker_ready(sender=None, conf=None, instance=None, **kwargs):
    server_url = os.environ.get('SERVER_URL', 'http://server').rstrip('/')
    server_port = os.environ.get('SERVER_PORT', '1456')

    LOGGER.info(f'configured server: {server_url}:{server_port}')
    LOGGER.info('worker ready to accept tasks')


@worker_shutdown.connect
def config_worker_shutdown(sender=None, conf=None, instance=None, **kwargs):
    LOGGER.info('worker shutdown completed')


if __name__ == '__main__':
    worker.start()
