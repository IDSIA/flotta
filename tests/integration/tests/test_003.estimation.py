from ferdelance.schemas.queries import Query, QueryEstimate
from ferdelance.workbench.context import Context
from ferdelance.logging import get_logger

import os
import sys

LOGGER = get_logger(__name__)

if __name__ == "__main__":
    project_id: str = os.environ.get("PROJECT_ID", "")
    server: str = os.environ.get("SERVER", "")

    if not project_id:
        LOGGER.info("Project id not found")
        sys.exit(-1)

    if not server:
        LOGGER.info("Server host not found")
        sys.exit(-1)

    ctx: Context = Context(server)

    project = ctx.project(project_id)

    q: Query = project.extract()
    e_mean: QueryEstimate = q.mean(q["HouseAge"])

    mean = ctx.execute(project, e_mean)

    LOGGER.info(mean.mean)
