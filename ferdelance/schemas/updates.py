from pydantic import BaseModel


class UpdateData(BaseModel):
    """Basic update response from the server with the next action to do."""

    action: str


class UpdateExecute(UpdateData):
    """Task that the client has to execute next."""

    artifact_id: str
    job_id: str


class UpdateNothing(UpdateData):
    """Nothing else to do."""

    pass
