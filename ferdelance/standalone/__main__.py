from ferdelance.config import conf
from ferdelance.standalone.processes import (
    LocalWorker,
    LocalClient,
    LocalServer,
)

from multiprocessing import Queue
from multiprocessing.managers import BaseManager

import logging

LOGGER = logging.getLogger(__name__)


if __name__ == '__main__':
    LOGGER.info('standalone application starting')

    conf.STANDALONE = True
    conf.SERVER_MAIN_PASSWORD = '7386ee647d14852db417a0eacb46c0499909aee90671395cb5e7a2f861f68ca1'  # this is a dummy key
    conf.DB_DIALECT = 'sqlite'
    conf.DB_MEMORY = True
    conf.SERVER_INTERFACE = '0.0.0.0'

    aggregation_queue = Queue()

    manager = BaseManager(address=('', 14560))
    manager.register('get_queue', callable=lambda: aggregation_queue)
    manager.start()

    server_process = LocalServer()
    worker_process = LocalWorker()
    client_process = LocalClient()

    server_process.start()
    worker_process.start()
    client_process.start()

    server_process.join()
    worker_process.join()
    client_process.join()

    manager.shutdown()

    LOGGER.info('standalone application terminated')
