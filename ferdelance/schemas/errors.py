from pydantic import BaseModel


class TaskError(BaseModel):
    job_id: str = ""
    message: str = ""
    stack_trace: str = ""
