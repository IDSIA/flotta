from ferdelance.config import config_manager
from ferdelance.server.deployment import start_server

import ray


if __name__ == "__main__":
    ray.init()

    start_server(config_manager.get())
