from __future__ import annotations

from pydantic import BaseModel


class ServerJoinRequest(BaseModel):
    name: str

    system: str
    mac_address: str
    node: str

    public_key: str  # b64encoded bytes
    version: str


class ServerJoinData(BaseModel):
    id: str
    token: str
    public_key: str
