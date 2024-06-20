__all__ = [
    "client_router",
    "node_router",
    "task_router",
    "resource_router",
    "workbench_router",
]

from flotta.node.routes.client import client_router
from flotta.node.routes.node import node_router
from flotta.node.routes.task import task_router
from flotta.node.routes.workbench import workbench_router
from flotta.node.routes.resources import resource_router
