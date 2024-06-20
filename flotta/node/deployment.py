from ferdelance.config import Configuration
from ferdelance.logging import get_logger
from ferdelance.node.api import api

from ray.serve.handle import DeploymentHandle
from ray import serve
import ray

from time import sleep

import httpx

LOGGER = get_logger(__name__)


@serve.deployment(name="api")
@serve.ingress(api)
class ServerWrapper:
    pass


def start_node(configuration: Configuration, name: str = "Ferdelance_node") -> DeploymentHandle:
    LOGGER.info(f"creating server at host={configuration.node.interface} port={configuration.node.port}")

    return serve.run(
        ServerWrapper.bind(),
        host=configuration.node.interface,
        port=configuration.node.port,
        name=name,
        route_prefix="/",
    )


def wait_node(config: Configuration, c: DeploymentHandle):
    # This is an horrid way to keep this script (and ray) alive...
    while True:
        sleep(config.node.healthcheck)
        try:
            res = httpx.get(f"{config.url_deploy()}/")
            res.raise_for_status()
        except Exception as e:
            LOGGER.error(e)
            LOGGER.exception(e)
        except KeyboardInterrupt:
            # TODO: should we use handlers?
            break
