from ferdelance.config import Configuration
from ferdelance.server.api import api

from ray.serve.handle import RayServeSyncHandle
from ray import serve


@serve.deployment(
    route_prefix="/",
    name="Ferdelance_api",
)
@serve.ingress(api)
class ServerWrapper:
    pass


def start_server(configuration: Configuration, name: str = "Ferdelance_server") -> RayServeSyncHandle | None:
    return serve.run(
        ServerWrapper.bind(),
        host=configuration.server.interface,
        port=configuration.server.port,
        name=name,
    )
