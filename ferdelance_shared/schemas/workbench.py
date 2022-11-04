from pydantic import BaseModel


class WorkbenchJoinRequest(BaseModel):
    """Data sent by the workbench to join the server."""
    public_key: str


class WorkbenchJoinData(BaseModel):
    """Data sent by the server to a workbench after a successful join."""
    id: str
    token: str
    public_key: str
