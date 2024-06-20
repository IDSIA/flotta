from flotta.core import Artifact
from flotta.core.estimators import MeanEstimator
from flotta.core.queries import Query
from flotta.logging import get_logger
from flotta.workbench.context import Context

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

    resources.sort(key=lambda x: x.creation_time.timestamp() if x.creation_time else -1)

    for r in resources:
        LOGGER.info(f"resource: {r.resource_id} produced by {r.producer_id} at {r.creation_time}")

    assert len(resources) == 4

    mean = ctx.get_resource(resources[-1])

    LOGGER.info(mean)
