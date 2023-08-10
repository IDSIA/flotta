from ferdelance.workbench.context import Context

import os
import sys


if __name__ == "__main__":
    project_id: str = os.environ.get("PROJECT_ID", "")
    server: str = os.environ.get("SERVER", "")

    if not project_id:
        print("Project id not found")
        sys.exit(-1)

    if not server:
        print("Server host not found")
        sys.exit(-1)

    ctx = Context(server)

    project = ctx.project(project_id)

    print(project)

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
