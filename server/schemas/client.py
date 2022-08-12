from pydantic import BaseModel


class ClientJoinRequest(BaseModel):
    system: str
    mac_address: str
    node: str

    public_key: str
    version: str


class ClientJoinResponse(BaseModel):
    uuid: str
    token: str
    public_key: str


class ClientLeaveRequest(BaseModel):
    pass


class ClientLeaveResponse(BaseModel):
    pass


class ClientUpdateRequest(BaseModel):
    pass


class ClientUpdateResponse(BaseModel):
    pass
