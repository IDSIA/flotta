from pydantic import BaseModel
from datetime import datetime


class FeatureBase(BaseModel):
    feature_id: str

    name: str
    dtype: str

    v_mean: float
    v_std: float
    v_min: float
    v_p25: float
    v_p50: float
    v_p75: float
    v_max: float
    v_miss: float

    n_cats: int


class FeatureCreate(FeatureBase):
    creation_time: datetime
    update_time: datetime
    removed: bool
