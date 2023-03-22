from pydantic import BaseModel


class ErrorArtifact(BaseModel):
    artifact_id: str
    message: str = ""
    stack_trace: str = ""
