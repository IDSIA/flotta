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
    is_local: bool = False

    @root_validator
    def force_localhost(cls, values):
        if values["is_local"]:
            values["url"] = "localhost"
        return values


class TaskResource(TaskNode):
    """Identifies a resource and its location inside the node network."""

    artifact_id: str
    job_id: str
    resource_id: str


class Task(BaseModel):
    artifact_id: str
    job_id: str  # current job
    project_id: str

    iteration: int

    step: Step  # what execute

    task_resources: list[TaskResource]  # what collect from

    next_nodes: list[TaskNode]  # receivers of the produced resources

    def run(self, env: Environment) -> Environment:
        return self.step.step(env)


class TaskDone(BaseModel):
    artifact_id: str
    job_id: str


class TaskError(BaseModel):
    job_id: str = ""
    message: str = ""
    stack_trace: str = ""
