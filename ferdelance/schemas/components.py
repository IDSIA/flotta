from datetime import datetime
from pydantic import BaseModel


class Component(BaseModel):
    id: str

    type_name: str
    name: str = ""

    public_key: str

    active: bool
    left: bool


dummy = Component(
    id="dummy",
    type_name="DUMMY",
    name="dummy",
    public_key="",
    active=False,
    left=False,
)


class Client(Component):
    version: str

    machine_system: str
    machine_mac_address: str
    machine_node: str

    blacklisted: bool
    ip_address: str


class Token(BaseModel):
    id: int
    component_id: str
    token: str
    creation_time: datetime
    expiration_time: float
    valid: bool


class Event(BaseModel):
    id: int
    component_id: str
    time: datetime
    event: str


class Application(BaseModel):
    id: str
    creation_time: datetime
    version: str
    active: bool
    path: str
    name: str
    description: str | None
    checksum: str
