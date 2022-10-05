from celery import Celery
from celery.signals import worker_shutdown, celeryd_init

import os

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
def configure_workers(sender=None, conf=None, instance=None, **kwargs):
    print('STARTUP')
    os.environ['testing'] = 'asdasdasd'


@ worker_shutdown.connect
def configure_workers(sender=None, conf=None, instance=None, **kwargs):
    print(os.environ.get('testing', 'nothing'))
    print('SHUTDOWN')


if __name__ == '__main__':
    worker.start()
