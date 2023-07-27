from ferdelance.config import conf
from ferdelance.server.api import api

from ray import serve

import ray


@serve.deployment(
    route_prefix="/",
    name="Ferdelance_api",
)
@serve.ingress(api)
class FastAPIWrapper:
    pass


if __name__ == "__main__":
    ray.init()

    serve.run(
        FastAPIWrapper.bind(),
        host=conf.SERVER_INTERFACE,
        port=conf.SERVER_PORT,
        name="Ferdelance_server",
    )
