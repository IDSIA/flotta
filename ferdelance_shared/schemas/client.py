from pydantic import BaseModel
from datetime import datetime


class ClientJoinRequest(BaseModel):
    """Data sent by the client to join the server."""
    system: str
    mac_address: str
    node: str

    public_key: str  # b64encoded bytes
    version: str


class ClientJoinData(BaseModel):
    """Data sent by the server to the client after a successful join."""
    id: str
    token: str
    public_key: str


class ClientDetails(BaseModel):
    client_id: str
    created_at: datetime
    version: str


class ClientUpdate(BaseModel):
    action: str


class ClientUpdateTaskCompleted(ClientUpdate):
    client_task_id: str
    # TODO: consider return errors to workbench
