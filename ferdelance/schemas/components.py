from datetime import datetime
from pydantic import BaseModel


class BaseComponent(BaseModel):
    type_name: str
    name: str = ""

    public_key: str

    active: bool
    left: bool


class Component(BaseComponent):
    component_id: str


dummy = Component(
    type_name="DUMMY",
    name="dummy",
    public_key="",
    active=False,
    left=False,
    component_id="dummy",
)


class Client(BaseComponent):
    client_id: str

    version: str

    machine_system: str
    machine_mac_address: str
    machine_node: str

    blacklisted: bool
    ip_address: str


class Token(BaseModel):
    token_id: int
    component_id: str
    token: str
    creation_time: datetime
    expiration_time: float
    valid: bool


class Event(BaseModel):
    component_id: str
    event_id: int
    event_time: datetime
    event: str


class Application(BaseModel):
    app_id: str
    creation_time: datetime
    version: str
    active: bool
    path: str
    name: str
    description: str | None
    checksum: str
