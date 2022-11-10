from pydantic import BaseModel


class Client(BaseModel):
    client_id: str

    version: str
    public_key: str

    machine_system: str
    machine_mac_address: str
    machine_node: str

    type: str

    active: bool
    blacklisted: bool
    left: bool
    ip_address: str


class User(BaseModel):
    user_id: str
    public_key: str
    active: bool
    left: bool
