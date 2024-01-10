from ferdelance.core.entity import Entity
from ferdelance.core.environment import Environment
from ferdelance.core.interfaces import Step

from pydantic import BaseModel, root_validator


class TaskRequest(BaseModel):
    """Schema to request the current context to use."""

    artifact_id: str
    job_id: str  # job to fetch


class TaskNode(BaseModel):
    """Identifies a location inside the node network."""

    component_id: str
    public_key: str
    url: str
    available_locally: bool = False


class TaskResource(TaskNode):
    """Identifies a resource and its location inside the node network."""

    resource_id: str
    artifact_id: str
    job_id: str
    iteration: int
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
