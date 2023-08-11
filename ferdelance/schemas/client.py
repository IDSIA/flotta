from pydantic import BaseModel


class ClientDetails(BaseModel):
    id: str
    name: str
    version: str


class ClientUpdate(BaseModel):
    action: str


class ClientUpdateTaskCompleted(ClientUpdate):
    client_task_id: str
    # TODO: consider return errors to workbench
