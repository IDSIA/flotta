from pydantic import BaseModel


class ResourceRequest(BaseModel):
    artifact_id: str
    resource_id: str
