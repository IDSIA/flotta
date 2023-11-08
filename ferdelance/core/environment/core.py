from typing import Any

from pandas import DataFrame


class Environment:
    artifact_id: str
    project_id: str

    label: str

    X_tr: DataFrame | None = None
    y_tr: DataFrame | None = None
    X_ts: DataFrame | None = None
    y_ts: DataFrame | None = None

    env: dict[str, Any] = dict()

    def __getitem__(self, key: str) -> Any:
        return self.env[key]

    def __setitem__(self, key: str, value: Any) -> None:
        self.env[key] = value
