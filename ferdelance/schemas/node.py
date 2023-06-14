from pydantic import BaseModel


class JoinRequest(BaseModel):
    """Data required to join the server."""

    name: str

    system: str
    mac_address: str
    node: str

    public_key: str  # b64encoded bytes
    version: str


class JoinData(BaseModel):
    """Data returned to the node after a successful join."""

    id: str
    token: str
    public_key: str
