from ferdelance.schemas.artifacts import Artifact

from pydantic import BaseModel


class WorkerTask(BaseModel):
    artifact: Artifact
    job_id: str
    result_ids: list[str]
