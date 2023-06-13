from pydantic import BaseModel


class ErrorArtifact(BaseModel):
    artifact_id: str
    job_id: str
    message: str = ""
    stack_trace: str = ""
