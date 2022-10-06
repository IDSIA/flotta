from celery import Celery
import os

worker = Celery(
    'ferdelance',
    backend=os.getenv('CELERY_BACKEND_URL'),
    broker=os.getenv('CELERY_BROKER_URL'),
    include=['ferdelance.worker.tasks'],
)

worker.conf.update(
    result_expires=3600,
)

if __name__ == '__main__':
    worker.start()
