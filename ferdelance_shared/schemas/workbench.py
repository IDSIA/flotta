from pydantic import BaseModel


class WorkbenchJoinData(BaseModel):
    token: str
