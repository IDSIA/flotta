from datetime import datetime
from pydantic import BaseModel


class Component(BaseModel):
    id: str

    name: str = ""
    type_name: str

    public_key: str

    active: bool = True
    blacklisted: bool = False
    left: bool = False

    version: str = ""

    url: str = ""
    ip_address: str = ""


class Event(BaseModel):
    id: int
    component_id: str
    time: datetime
    event: str
