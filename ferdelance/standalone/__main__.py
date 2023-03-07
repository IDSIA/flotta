from ferdelance.config import conf
from ferdelance.client.arguments import setup_config_from_arguments
from ferdelance.server.api import api
from ferdelance.standalone.processes import LocalClient, LocalServer, LocalWorker
from ferdelance.standalone.extra import extra

from multiprocessing import Queue

import logging
import signal
import uvicorn

LOGGER = logging.getLogger(__name__)


if __name__ == "__main__":
    from multiprocessing import set_start_method

    set_start_method("spawn")

    client_conf = setup_config_from_arguments()

    LOGGER.info("standalone application starting")

    conf.STANDALONE = True
    conf.SERVER_MAIN_PASSWORD = (
        "7386ee647d14852db417a0eacb46c0499909aee90671395cb5e7a2f861f68ca1"  # this is a dummy key
    )

    conf.DB_DIALECT = "sqlite"
    conf.DB_HOST = "./sqlite.db"
    conf.SERVER_INTERFACE = "0.0.0.0"
    conf.STANDALONE_WORKERS = 7
    conf.PROJECT_DEFAULT_TOKEN = (
        "58981bcbab77ef4b8e01207134c38873e0936a9ab88cd76b243a2e2c85390b94"  # this is a standard
    )

    extra.aggregation_queue = Queue()

    worker_process = LocalWorker(extra.aggregation_queue)
    client_process = LocalClient(client_conf)

    server = LocalServer(
        config=uvicorn.Config(
            api,
            host=conf.SERVER_INTERFACE,
            port=conf.SERVER_PORT,
        )
    )

    def handler(signalname):
        def f(signal_received, frame):
            raise KeyboardInterrupt(f"{signalname}: stop received")

        return f

    signal.signal(signal.SIGINT, handler("SIGINT"))
    signal.signal(signal.SIGTERM, handler("SIGTERM"))

    with server.run_in_thread():
        try:
            worker_process.start()
            client_process.start()

        except KeyboardInterrupt:
            LOGGER.info("stopping...")

        try:
            client_process.join()
            worker_process.join()
        except KeyboardInterrupt:
            LOGGER.info("stopping more...")

    LOGGER.info("standalone application terminated")
