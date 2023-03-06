from ferdelance.client.arguments import setup_config_from_arguments
from ferdelance.standalone.processes import LocalClient, LocalServer, LocalWorker
from ferdelance.standalone.extra import extra

from multiprocessing import Queue

import os
import logging
import signal

LOGGER = logging.getLogger(__name__)


if __name__ == "__main__":
    from multiprocessing import set_start_method

    set_start_method("spawn")

    client_conf = setup_config_from_arguments()

    LOGGER.info("standalone application starting")

    os.environ["STANDALONE"] = "True"
    os.environ[
        "SERVER_MAIN_PASSWORD"
    ] = "7386ee647d14852db417a0eacb46c0499909aee90671395cb5e7a2f861f68ca1"  # this is a dummy key
    os.environ["DB_DIALECT"] = "sqlite"
    os.environ["DB_HOST"] = "./sqlite.db"
    os.environ["SERVER_INTERFACE"] = "0.0.0.0"
    os.environ["STANDALONE_WORKERS"] = "7"
    os.environ["PROJECT_DEFAULT_TOKEN"] = "58981bcbab77ef4b8e01207134c38873e0936a9ab88cd76b243a2e2c85390b94"

    aggregation_queue = Queue()

    extra.AGGREGATION_QUEUE = aggregation_queue

    server_process = LocalServer()
    worker_process = LocalWorker(aggregation_queue)
    client_process = LocalClient(client_conf)

    def handler(signalname):
        def f(signal_received, frame):
            raise KeyboardInterrupt(f"{signalname}: stop received")

        return f

    signal.signal(signal.SIGINT, handler("SIGINT"))
    signal.signal(signal.SIGTERM, handler("SIGTERM"))

    try:
        server_process.start()
        worker_process.start()
        client_process.start()

    except KeyboardInterrupt:
        LOGGER.info("stopping...")

    finally:
        client_process.join()
        worker_process.join()
        server_process.join()

    LOGGER.info("standalone application terminated")
