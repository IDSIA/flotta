from ferdelance.schemas.artifacts import Artifact
from ferdelance.schemas.estimators import (
    GroupCountEstimator,
    MeanEstimator,
)
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

    project = ctx.load(project_id)

    q = project.extract()
    e_mean = q.add_estimator(MeanEstimator(q["HouseAge"]))

    mean = ctx.execute(project, e_mean)

    print(mean.mean)
