from datetime import datetime
from pydantic import BaseModel


class Component(BaseModel):
    id: str

    name: str = ""
    type_name: str

    public_key: str

    active: bool
    blacklisted: bool
    left: bool

    version: str

    url: str
    ip_address: str


dummy = Component(
    id="dummy",
    type_name="DUMMY",
    name="dummy",
    public_key="",
    active=False,
    left=False,
    version="0.0",
    blacklisted=False,
    ip_address="127.0.0.1",
    url="http://localhost/",
)


class Event(BaseModel):
    id: int
    component_id: str
    time: datetime
    event: str
