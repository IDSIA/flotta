from ferdelance.config import conf
from ferdelance.server.api import api

from ferdelance.client import FerdelanceClient
from multiprocessing import Process

import logging
import random
import signal
import time
import uvicorn


LOGGER = logging.getLogger(__name__)


def run_server():
    uvicorn.run(api, host='0.0.0.0', port=1456)


def run_client():
    time.sleep(3)  # this will give the server time to start

    LOGGER.info('starting client')

    mac_address: str = "02:00:00:%02x:%02x:%02x" % (random.randint(0, 255), random.randint(0, 255), random.randint(0, 255))
    node: str = str(1000000000000 + int(random.uniform(0, 1.0) * 1000000000))

    client = FerdelanceClient('http://localhost:1456', heartbeat=1, machine_mac_addres=mac_address, machine_node=node)

    def main_signal_handler(signum, frame):
        """This handler is used to gracefully stop when ctrl-c is hit in the terminal."""
        client.stop_loop()

    signal.signal(signal.SIGINT, main_signal_handler)
    signal.signal(signal.SIGTERM, main_signal_handler)

    client.run()

    LOGGER.info('terminate client')


if __name__ == '__main__':
    LOGGER.info('standalone application starting')

    conf.STANDALONE = True
    conf.SERVER_MAIN_PASSWORD = '7386ee647d14852db417a0eacb46c0499909aee90671395cb5e7a2f861f68ca1'  # this is a dummy key
    conf.DB_DIALECT = 'sqlite'
    conf.DB_MEMORY = True

    server_process = Process(target=run_server)
    client_process = Process(target=run_client)

    server_process.start()
    client_process.start()

    server_process.join()
    client_process.join()

    LOGGER.info('standalone application terminated')
