# %%
from ferdelance.schemas.artifacts import Artifact
from ferdelance.schemas.estimators import (
    GroupCountEstimator,
    MeanEstimator,
)
from ferdelance.workbench.context import Context

import pandas as pd

import pickle
import time
import os
import sys

# %%
if __name__ == "__main__":

    # %%
    os.environ["PROJECT_ID"] = "58981bcbab77ef4b8e01207134c38873e0936a9ab88cd76b243a2e2c85390b94"
    os.environ["SERVER"] = "http://localhost:1456"

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
    e_mean = q.add_estimator(MeanEstimator(q["sepal.length"]))

    mean = ctx.execute(project, e_mean)

    print(mean.mean)

# %%
