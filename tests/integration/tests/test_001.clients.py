from ferdelance.workbench.context import Context
from ferdelance.logging import get_logger

import os
import sys


LOGGER = get_logger(__name__)

if __name__ == "__main__":
    project_id: str = os.environ.get("PROJECT_ID", "")
    server: str = os.environ.get("SERVER", "")

    LOGGER.info(f"PROJECT_ID: {project_id}")
    LOGGER.info(f"SERVER:     {server}")

    if not project_id:
        LOGGER.info("Project id not found")
        sys.exit(-1)

    if not server:
        LOGGER.info("Server host not found")
        sys.exit(-1)

    ctx = Context(server)

    project = ctx.project(project_id)

    LOGGER.info(project)

    if project.n_clients != 2:
        LOGGER.info(f"Invalid number of clients, expected 2 found {project.n_clients}")
        sys.exit(-1)

    ds = project.extract()

    features = [f.name for f in ds.features()]

    n_features = len(features)

    if n_features != 9:
        LOGGER.info("Invalid number of features, expected 2 found {n_features}")
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
            LOGGER.info(f"Feature {f} is missing")
            sys.exit(-1)

    LOGGER.info("Expected values achieved.")
