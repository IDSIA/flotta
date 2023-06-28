from pydantic import BaseModel


class ClientTaskError(BaseModel):
    job_id: str = ""
    message: str = ""
    stack_trace: str = ""


class WorkerAggregationJobError(BaseModel):
    job_id: str = ""
    message: str = ""
    stack_trace: str = ""
