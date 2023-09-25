from ferdelance.schemas.artifacts import Artifact
from ferdelance.schemas.models import (
    FederatedRandomForestClassifier,
    StrategyRandomForestClassifier,
    ParametersRandomForestClassifier,
)
from ferdelance.schemas.plans import TrainTestSplit
from ferdelance.workbench.context import Context
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
        LOGGER.info("Project id not found")
        sys.exit(-1)

    if not server:
        LOGGER.info("Server host not found")
        sys.exit(-1)

    ctx = Context(server)

    project = ctx.project(project_id)

    clients = ctx.clients(project)

    client_id_1, client_id_2 = [c.id for c in clients]

    q = project.extract()

    q = q.add_plan(TrainTestSplit("MedHouseValDiscrete", 0.2))

    q = q.add_model(
        FederatedRandomForestClassifier(
            strategy=StrategyRandomForestClassifier.MERGE,
            parameters=ParametersRandomForestClassifier(n_estimators=10),
        )
    )

    a: Artifact = ctx.submit(project, q)

    LOGGER.info(f"Artifact id: {a.id}")

    last_state = ""

    start_time = time.time()
    max_wait, wait_time = 60, 10

    while (status := ctx.status(a)).status != "COMPLETED":
        if status.status == last_state:
            LOGGER.info(".")
        else:
            last_state = status.status
            start_time = time.time()
            LOGGER.info(last_state)

        time.sleep(wait_time)

        if time.time() - start_time > max_wait:
            LOGGER.info("reached max wait time")
            sys.exit(-1)

    LOGGER.info("done!")

    cls_agg = ctx.get_result(a)

    LOGGER.info(f"aggregated model fetched: {cls_agg}")

    cls_pa1 = ctx.get_partial_result(a, client_id_1, 0)

    LOGGER.info(f"partial model 1 fetched:  {cls_pa1}")

    cls_pa2 = ctx.get_partial_result(a, client_id_2, 0)

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
