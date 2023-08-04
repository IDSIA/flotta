from ferdelance.config.arguments import setup_config_from_arguments
from ferdelance.server.deployment import start_server

import ray


if __name__ == "__main__":
    config, _ = setup_config_from_arguments()
    config.dump()

    ray.init()

    start_server(config)
