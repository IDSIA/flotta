from typing import Any
from dataclasses import dataclass, field

from pandas import DataFrame


@dataclass
class Environment:
    artifact_id: str
    project_token: str

    label: str = ""

    df: DataFrame | None = None

    X_tr: DataFrame | None = None
    Y_tr: DataFrame | None = None
    X_ts: DataFrame | None = None
    Y_ts: DataFrame | None = None

    env: dict[str, Any] = field(default_factory=dict)

    produced_resource_path: str | None = None

    def __getitem__(self, key: str) -> Any:
        return self.env[key]

    def __setitem__(self, key: str, value: Any) -> None:
        self.env[key] = value
