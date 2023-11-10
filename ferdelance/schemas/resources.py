from pydantic import BaseModel


class ResourceRequest(BaseModel):
    artifact_id: str
    job_id: str
    resource_id: str


class ResourceIdentifier(BaseModel):
    artifact_id: str
    job_id: str
    file: str = "attached"
