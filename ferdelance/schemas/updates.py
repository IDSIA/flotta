from pydantic import BaseModel


class UpdateData(BaseModel):
    """Basic update response from the server with the next action to do."""

    action: str
    artifact_id: str = ""
    job_id: str = ""
