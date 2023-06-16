from ferdelance.schemas.estimators import MeanEstimator
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

    q = project.extract()
    e_mean = q.add_estimator(MeanEstimator(q["HouseAge"]))

    mean = ctx.execute(project, e_mean)

    print(mean.mean)
