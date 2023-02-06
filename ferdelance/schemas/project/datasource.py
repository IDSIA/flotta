from pydantic import BaseModel
from datetime import datetime


class DataSourceBase(BaseModel):
    datasource_id: str
    datasource_hash: str

    name: str

    creation_time: datetime
    update_time: datetime
    removed: datetime

    n_records: int
    n_features: int


class DataSourceCreate(DataSourceBase):
    pass
