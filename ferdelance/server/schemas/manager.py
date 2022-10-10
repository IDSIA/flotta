from pydantic import BaseModel


class ManagerUploadClientResponse(BaseModel):
    upload_id: str
    filename: str


class ManagerUploadClientMetadataRequest(BaseModel):
    upload_id: str
    version: str | None
    name: str | None
    desc: str | None
    active: bool


class ManagerDownloadModelRequest(BaseModel):
    model_id: str
