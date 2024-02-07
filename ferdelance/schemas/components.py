from datetime import datetime
from pydantic import BaseModel, validator


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

    @validator("url")
    def url_must_end_without_slash(cls, v: str):
        return v.rstrip("/")


class Event(BaseModel):
    id: int
    component_id: str
    time: datetime
    event: str
