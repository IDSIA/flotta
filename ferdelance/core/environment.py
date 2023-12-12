from typing import Any
from dataclasses import dataclass, field

from pathlib import Path
from pandas import DataFrame

import pickle


@dataclass
class EnvResource:
    id: str
    path: Path
    data: Any | None = None

    def get(self) -> Any:
        if self.data is None:
            with open(self.path, "rb") as f:
                self.data = pickle.load(f)
        return self.data

    def store(self) -> None:
        if self.data is not None:
            with open(self.path, "wb") as f:
                pickle.dump(self.data, f)


@dataclass
class Environment:
    artifact_id: str
    project_token: str

    working_dir: Path | None = None

    label: str = ""

    df: DataFrame | None = None

    X_tr: DataFrame | None = None
    Y_tr: DataFrame | None = None
    X_ts: DataFrame | None = None
    Y_ts: DataFrame | None = None

    env: dict[str, Any] = field(default_factory=dict)

    stored_resources: dict[str, EnvResource] = field(default_factory=dict)
    produced_resource: EnvResource | None = None

    def __getitem__(self, key: str) -> Any:
        return self.env[key]

    def __setitem__(self, key: str, value: Any) -> None:
        self.env[key] = value

    def add_resource(self, resource_id: str, path: Path) -> None:
        """Add a resource with an assigned path on disk."""
        self.stored_resources[resource_id] = EnvResource(
            id=resource_id,
            path=path,
        )

    def set_product(self, data: Any) -> None:
        if self.produced_resource is None:
            raise ValueError("Unexpected produced resource: non is assigned")
        self.produced_resource.data = data
