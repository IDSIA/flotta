from pydantic import BaseModel
from datetime import datetime


class ProjectBase(BaseModel):
    project_id: str
    name: str
    creation_time: datetime


class ProjectCreate(ProjectBase):
    token: str
