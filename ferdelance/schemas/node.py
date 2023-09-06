from pydantic import BaseModel

from ferdelance.schemas.components import Component
from ferdelance.schemas.metadata import Metadata


class NodeJoinRequest(BaseModel):
    """Data required to join the server."""

    id: str

    name: str
    type_name: str

    public_key: str  # b64encoded bytes
    version: str

    url: str = ""

    # for signature validation
    checksum: str
    signature: str


class JoinData(BaseModel):
    """Data returned to the node after a successful join."""

    component: Component

    # TODO: list of other nodes (no clients) with projects and metadata (?)
    nodes: list[Component]


class ServerPublicKey(BaseModel):
    public_key: str


class NodeMetadata(BaseModel):
    id: str
    metadata: Metadata
