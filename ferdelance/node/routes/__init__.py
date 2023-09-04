__all__ = [
    "client_router",
    "node_router",
    "task_router",
    "workbench_router",
]

from ferdelance.node.routes.client import client_router
from ferdelance.node.routes.node import node_router
from ferdelance.node.routes.task import task_router
from ferdelance.node.routes.workbench import workbench_router
