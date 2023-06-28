__all__ = [
    "client_router",
    "node_router",
    "server_router",
    "workbench_router",
    "worker_router",
]

from ferdelance.server.routes.client import client_router
from ferdelance.server.routes.node import node_router
from ferdelance.server.routes.server import server_router
from ferdelance.server.routes.workbench import workbench_router
from ferdelance.server.routes.worker import worker_router
