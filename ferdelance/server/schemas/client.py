from pydantic import BaseModel


class ClientJoinRequest(BaseModel):
    system: str
    mac_address: str
    node: str

    public_key: str  # b64encoded bytes
    version: str


class ClientJoinResponse(BaseModel):
    id: str          # b64encoded bytes
    token: str       # b64encoded bytes
    public_key: str


class ClientLeaveRequest(BaseModel):
    pass


class ClientLeaveResponse(BaseModel):
    pass


class ClientUpdateRequest(BaseModel):
    payload: str     # b64encoded bytes


class ClientUpdateResponse(BaseModel):
    payload: str     # b64encoded bytes


class ClientUpdateModelRequest(BaseModel):
    payload: str     # b64encoded bytes


class ClientUpdateMetadataRequest(BaseModel):
    payload: str     # binary


class ClientTaskRequest(BaseModel):
    payload: str


class ClientTaskResponse(BaseModel):
    payload: str
