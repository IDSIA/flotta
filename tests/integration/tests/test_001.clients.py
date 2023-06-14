from ferdelance.workbench.context import Context

import os
import sys


if __name__ == "__main__":
    project_id: str | None = os.environ.get("PROJECT_ID", None)
    server: str | None = os.environ.get("SERVER")

    if project_id is None:
        print("Project id not found")
        sys.exit(-1)

    if server is None:
        print("Server host not found")
        sys.exit(-1)

    ctx = Context(server)

    project = ctx.project(project_id)

    if project.n_clients != 2:
        print("Invalid number of clients, expected 2 found", project.n_clients)
        sys.exit(-1)

    ds = project.extract()

    features = [f.name for f in ds.features()]

    n_features = len(features)

    if n_features != 9:
        print("Invalid number of features, expected 2 found", n_features)
        sys.exit(-1)

    expected_features = [
        "MedInc",
        "HouseAge",
        "AveRooms",
        "AveBedrms",
        "Population",
        "AveOccup",
        "Latitude",
        "Longitude",
        "MedHouseValDiscrete",
    ]

    for f in expected_features:
        if f not in features:
            print("Feature", f, "is missing")
            sys.exit(-1)

    print("Expected values achieved.")
