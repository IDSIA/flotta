from typing import Any
from dataclasses import dataclass, field

from pathlib import Path
from pandas import DataFrame

import os
import pickle


@dataclass
class EnvResource:
    # resource_id
    id: str
    # where is located on disk
    path: Path = Path(".")

    def get(self) -> Any:
        with open(self.path, "rb") as f:
            return pickle.load(f)


@dataclass
class EnvProduct:
    # resource_id
    id: str
    # resource name
    name: str
    # data created
    data: Any = None


@dataclass
class Environment:
    artifact_id: str
    project_token: str
    product_id: str

    working_dir: Path

    label: str = ""

    df: DataFrame | None = None

    X_tr: DataFrame | None = None
    Y_tr: DataFrame | None = None
    X_ts: DataFrame | None = None
    Y_ts: DataFrame | None = None

    # resource produced in this step that will stay locally
    locals: dict[str, Any] = field(default_factory=dict)

    _local_path: Path = field(init=False)

    # resources that has been created in previous steps
    resources: dict[str, EnvResource] = field(default_factory=dict)

    # resource produced in this step that will be sent to the next node(s)
    products: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        self._local_path = self.working_dir / ".." / "local.pkl"

        # load (if exists) local variables
        if os.path.exists(self._local_path):
            with open(self._local_path, "rb") as f:
                self.locals = pickle.load(f)

    def __getitem__(self, key: str) -> Any:
        """Returns values stored as a local variable or as a resource.

        Args:
            key (str):
                Key used to get the data. If starts with a dot ".", a local
                variable will be returned; otherwise the relative resource.

        Returns:
            Any: values stored as a local variable or as a resource.
        """
        if key.startswith("."):
            return self.locals[key]

        if key in self.resources:
            return self.resources[key].get()

        return self.products[key]

    def __setitem__(self, key: str, value: Any) -> None:
        """Sets values as local variable or products. Cannot be used to set
        remote resources.

        Args:
            key (str):
                Key used to set the data. If it starts with a dot ".", a local
                variable will be set; otherwise the product value will be set.
            value (Any):
                Value to set.
        """

        if key.startswith("."):
            self.locals[key] = value
        else:
            self.products[key] = value

    def add_resource(self, resource_id: str, path: Path) -> None:
        """Add a resource with an assigned path on disk."""
        res = EnvResource(
            id=resource_id,
            path=path,
        )

        self.resources[resource_id] = res

    def list_resource_ids(self) -> list[str]:
        return list(self.resources.keys())

    def product_path(self) -> Path:
        return self.working_dir / f"{self.product_id}.pkl"

    def store(self) -> None:
        # store local data
        with open(self._local_path, "wb") as f:
            pickle.dump(self.locals, f)

        # store product
        with open(self.product_path(), "wb") as f:
            pickle.dump(self.products, f)
