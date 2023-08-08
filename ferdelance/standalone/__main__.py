from ferdelance.config.arguments import setup_config_from_arguments
from ferdelance.client.client import start_client
from ferdelance.server.deployment import start_server

import ray

import logging
import random

LOGGER = logging.getLogger(f"{__package__}.{__name__}")


if __name__ == "__main__":
    LOGGER.info("standalone application starting")

    config, _ = setup_config_from_arguments()

    config.mode = "standalone"
    config.server.main_password = (
        "7386ee647d14852db417a0eacb46c0499909aee90671395cb5e7a2f861f68ca1"  # this is a dummy key
    )

    config.database.dialect = "sqlite"
    config.database.host = "./storage/sqlite.db"
    config.server.interface = "0.0.0.0"
    config.server.token_project_default = (
        "58981bcbab77ef4b8e01207134c38873e0936a9ab88cd76b243a2e2c85390b94"  # this is a dummy token
    )

    config.client.machine_mac_address = "02:00:00:%02x:%02x:%02x" % (
        random.randint(0, 255),
        random.randint(0, 255),
        random.randint(0, 255),
    )
    config.client.machine_node = str(1000000000000 + int(random.uniform(0, 1.0) * 1000000000))

    config.dump()

    # def handler(signalname):
    #     def f(signal_received, frame):
    #         raise KeyboardInterrupt(f"{signalname}: stop received")

    #     return f

    # signal.signal(signal.SIGINT, handler("SIGINT"))
    # signal.signal(signal.SIGTERM, handler("SIGTERM"))

    ray.init()

    start_server(config)
    h = start_client(config)

    ray.get(h)

    ray.shutdown()

    LOGGER.info("standalone application terminated")
