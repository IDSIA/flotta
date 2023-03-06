from ferdelance.config import conf
from ferdelance.client import FerdelanceClient
from ferdelance.client.config import Config
from ferdelance.client.exceptions import ClientExitStatus

from multiprocessing import Process

import logging
import random
import shutil
import signal
import time

LOGGER = logging.getLogger(__name__)


class LocalClient(Process):
    def __init__(self, conf: Config) -> None:
        super().__init__()
        self.client_conf = conf
        self.client = FerdelanceClient(self.client_conf)

    def run(self):
        time.sleep(3)  # this will give the server time to start

        LOGGER.info("starting client")

        self.client_conf.machine_mac_address = "02:00:00:%02x:%02x:%02x" % (
            random.randint(0, 255),
            random.randint(0, 255),
            random.randint(0, 255),
        )
        self.client_conf.machine_node = str(1000000000000 + int(random.uniform(0, 1.0) * 1000000000))

        self.client_conf.server = conf.server_url()

        try:
            exit_code = self.client.run()
        except KeyboardInterrupt:
            exit_code = 0

        except ClientExitStatus as e:
            exit_code = e.exit_code

        LOGGER.info(f"terminated application with exit_code={exit_code}")

        if conf.DB_MEMORY:
            # remove workdir since after shutdown the database content will be lost
            shutil.rmtree(self.client.config.workdir)
            LOGGER.info(f"client workdir={self.client.config.workdir} removed")
