from ferdelance.core import Artifact
from ferdelance.core.estimators import MeanEstimator
from ferdelance.core.queries import Query
from ferdelance.logging import get_logger
from ferdelance.workbench.context import Context

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

    me = MeanEstimator(query=q)

    artifact: Artifact = ctx.submit(project, me.get_steps())

    try:
        ctx.wait(artifact)
    except ValueError as e:
        LOGGER.exception(e)
        sys.exit(-1)

    LOGGER.info("done!")

    resources = ctx.list_resources(artifact)

    assert len(resources) == 1

    mean = ctx.get_resource(resources[0])

    LOGGER.info(mean)
