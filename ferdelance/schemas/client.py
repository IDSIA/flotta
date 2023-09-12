from pydantic import BaseModel


class ClientDetails(BaseModel):
    id: str
    name: str
    version: str


class ClientUpdate(BaseModel):
    action: str
