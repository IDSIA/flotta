from typing import Any
from ferdelance.config import conf
from ferdelance.client import FerdelanceClient

from multiprocessing import Process

import logging
import random
import shutil
import signal
import time

LOGGER = logging.getLogger(__name__)


class LocalClient(Process):

    def __init__(self, args: dict[str, Any]) -> None:
        super().__init__()

        self.args = args

    def run(self):
        time.sleep(3)  # this will give the server time to start

        LOGGER.info('starting client')

        mac_address: str = "02:00:00:%02x:%02x:%02x" % (random.randint(0, 255), random.randint(0, 255), random.randint(0, 255))
        node: str = str(1000000000000 + int(random.uniform(0, 1.0) * 1000000000))

        server_url = conf.server_url()

        workdir = self.args['workdir']
        heartbeat = self.args['heartbeat']
        leave = self.args['leave']
        datasources = self.args['datasources']

        client = FerdelanceClient(server_url, workdir, heartbeat, leave, datasources, mac_address, node)

        def main_signal_handler(signum, frame):
            """This handler is used to gracefully stop when ctrl-c is hit in the terminal."""
            client.stop_loop()

        signal.signal(signal.SIGINT, main_signal_handler)
        signal.signal(signal.SIGTERM, main_signal_handler)

        exit_code = client.run()

        LOGGER.info(f'terminated application with exit_code={exit_code}')

        if conf.DB_MEMORY:
            # remove workdir since after shutdown the database content will be lost
            shutil.rmtree(client.config.workdir)
            LOGGER.info(f'client workdir={client.config.workdir} removed')
