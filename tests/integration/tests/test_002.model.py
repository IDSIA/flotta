import resource
from ferdelance.core.artifacts import Artifact
from ferdelance.core.distributions import Collect
from ferdelance.core.model_operations import Aggregation, Train, TrainTest
from ferdelance.core.models import (
    FederatedRandomForestClassifier,
    StrategyRandomForestClassifier,
)
from ferdelance.core.steps import Finalize, Parallel
from ferdelance.core.transformers import FederatedSplitter
from ferdelance.workbench import Context
from ferdelance.logging import get_logger

from sklearn.metrics import confusion_matrix, accuracy_score, roc_auc_score, f1_score

import pandas as pd

import time
import os
import sys

LOGGER = get_logger(__name__)


def evaluate(model: FederatedRandomForestClassifier, x, y):
    pred = model.predict(x)
    y_pred = (pred > 0.5).astype("int")  # type: ignore

    f1 = f1_score(y, y_pred)
    ac = accuracy_score(y, y_pred)
    ra = roc_auc_score(y, pred)

    cm = confusion_matrix(y, pred)

    LOGGER.info(f"Accuracy: {ac}")
    LOGGER.info(f"F1:       {f1}")
    LOGGER.info(f"ROC AUC:  {ra}")

    LOGGER.info(f"\n{cm}")


if __name__ == "__main__":
    project_id: str = os.environ.get("PROJECT_ID", "")
    server: str = os.environ.get("SERVER", "")

    if not project_id:
        LOGGER.error("Project id not found")
        sys.exit(-1)

    if not server:
        LOGGER.error("Server host not found")
        sys.exit(-1)

    ctx = Context(server)

    project = ctx.project(project_id)

    clients = ctx.clients(project)

    client_id_1, client_id_2 = [c.id for c in clients]

    model = FederatedRandomForestClassifier(
        n_estimators=10,
        strategy=StrategyRandomForestClassifier.MERGE,
    )

    label = "MedHouseValDiscrete"

    steps = [
        Parallel(
            TrainTest(
                query=project.extract().add(
                    FederatedSplitter(
                        random_state=42,
                        test_percentage=0.2,
                        label=label,
                    )
                ),
                trainer=Train(model=model),
                model=model,
            ),
            Collect(),
        ),
        Finalize(
            Aggregation(model=model),
        ),
    ]

    artifact: Artifact = ctx.submit(project, steps)

    LOGGER.info(f"Artifact id: {artifact.id}")

    last_state = ""

    start_time = time.time()
    max_wait, wait_time = 60, 10

    while (status := ctx.status(artifact)).status != "COMPLETED":
        if status.status == last_state:
            LOGGER.info(".")
        else:
            last_state = status.status
            start_time = time.time()
            LOGGER.info(last_state)

        time.sleep(wait_time)

        if time.time() - start_time > max_wait:
            LOGGER.error("reached max wait time")
            sys.exit(-1)

    LOGGER.info("done!")

    resources = ctx.list_resources(artifact)

    if not len(resources) == 3:
        LOGGER.error("Not all models have been produced")
        sys.exit(-1)

    cls_agg = ctx.get_resource(artifact)

    LOGGER.info(f"aggregated model fetched: {cls_agg}")

    cls_pa1 = ctx.get_resource(artifact, client_id_1, 0)

    LOGGER.info(f"partial model 1 fetched:  {cls_pa1}")

    cls_pa2 = ctx.get_resource(artifact, client_id_2, 0)

    LOGGER.info(f"partial model 2 fetched:  {cls_pa2}")

    df = pd.read_csv("/data/california_housing.validation.csv")

    X = df.drop("MedHouseValDiscrete", axis=1).values
    Y = df["MedHouseValDiscrete"]

    LOGGER.info("Partial Model 1")
    evaluate(cls_pa1, X, Y)
    LOGGER.info("")

    LOGGER.info("Partial Model 2")
    evaluate(cls_pa2, X, Y)
    LOGGER.info("")

    LOGGER.info("Aggregated model")
    evaluate(cls_agg, X, Y)
    LOGGER.info("")
