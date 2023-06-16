from pydantic import BaseModel


class WorkerAggregationJobError(BaseModel):
    job_id: str = ""
    message: str = ""
    stack_trace: str = ""
