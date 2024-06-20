from pydantic import BaseModel

from ferdelance.schemas.components import Component
from ferdelance.schemas.metadata import Metadata


class NodeJoinRequest(BaseModel):
    """Data required to join the server."""

    id: str

    name: str
    type_name: str

    public_key: str
    version: str

    url: str = ""

    # for signature validation
    checksum: str
    signature: str


class JoinData(BaseModel):
    """Data returned to the node after a successful join."""

    component_id: str  # id of the joined component

    # TODO: list of other nodes (no clients) with projects and metadata (?)
    nodes: list[Component]


class NodePublicKey(BaseModel):
    public_key: str


class NodeMetadata(BaseModel):
    id: str
    metadata: Metadata
