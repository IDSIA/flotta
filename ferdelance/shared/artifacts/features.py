from pydantic import BaseModel


class BaseFeature(BaseModel):
    """Common information to all features."""
    name: str
    dtype: str | None

    v_mean: float | None
    v_std: float | None
    v_min: float | None
    v_p25: float | None
    v_p50: float | None
    v_p75: float | None
    v_max: float | None
    v_miss: float | None


class Feature(BaseFeature):
    """Information for the workbench."""
    feature_id: str
    datasource_id: str
    datasource_name: str


class MetaFeature(BaseFeature):
    """Information on features stored in the client."""
    removed: bool = False
