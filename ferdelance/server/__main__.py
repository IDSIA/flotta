from ferdelance.config import get_config
from ferdelance.server.deployment import start_server

import ray


if __name__ == "__main__":
    ray.init()

    conf = get_config()

    start_server(conf)
