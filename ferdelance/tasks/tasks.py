from unittest.util import strclass
from ferdelance.core.entity import Entity
from ferdelance.core.environment import Environment
from ferdelance.core.interfaces import Step

from pydantic import BaseModel


class TaskRequest(BaseModel):
    """Schema to request the current context to use."""

    artifact_id: str
    job_id: str  # job to fetch


class TaskNode(BaseModel):
    """Identifies a location inside the node network.

    The `target_id`, final receiver of the data, will receive data encrypted
    using its `target_public_key`. To reach the target use the proxy node
    identified by `proxy_id`, reachable at `proxy_url` by encrypting data with
    the given `proxy_public_key`. In the case that the proxy is the final
    receiver, then the target and proxy parameters will be the same.
    """

    target_id: str
    target_public_key: str
    target_url: str

    use_scheduler_as_proxy: bool


class TaskResource(BaseModel):
    """Identifies a resource and its location inside the node network.

    A resource can be identified by its unique `resource_id`, assigned to an
    `artifact_id` and a unique `job_id`. The `iteration` field defines which
    iteration in the artifact. When the resource is local to the execution node,
    the flag `available_locally` will be set to True and the `local_path` will be
    valid.
    """

    resource_id: str
    artifact_id: str
    job_id: str
    iteration: int

    component_id: str
    component_public_key: str
    component_url: str

    available_locally: bool = False
    local_path: str | None = None


class Task(Entity):
    project_token: str

    artifact_id: str
    job_id: str  # current job

    iteration: int

    step: Step  # what execute

    required_resources: list[TaskResource]  # what collect from
    next_nodes: list[TaskNode]  # receivers of the produced resources

    produced_resource_id: str  # id of the produced resource

    def run(self, env: Environment) -> Environment:
        return self.step.step(env)


class TaskDone(BaseModel):
    artifact_id: str
    job_id: str


class TaskError(BaseModel):
    job_id: str = ""
    message: str = ""
    stack_trace: str = ""
