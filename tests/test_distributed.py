from ferdelance.schemas.artifacts import Artifact
from ferdelance.schemas.models import (
    FederatedRandomForestClassifier,
    StrategyRandomForestClassifier,
    ParametersRandomForestClassifier,
)
from ferdelance.workbench.context import Context
from ferdelance.schemas.plans import TrainTestSplit

from sklearn.metrics import confusion_matrix, accuracy_score, roc_auc_score, f1_score

import pandas as pd

import time
import os


def evaluate(model: FederatedRandomForestClassifier, x, y):
    pred = model.predict(x)
    y_pred = (pred > 0.5).astype("int")

    f1 = f1_score(y, y_pred)
    ac = accuracy_score(y, y_pred)
    ra = roc_auc_score(y, pred)

    cm = confusion_matrix(y, pred)

    print("Accuracy:", ac)
    print("F1:      ", f1)
    print("ROC AUC: ", ra)

    print(cm)


# required docker version running
if __name__ == "__main__":

    ctx = Context(f'http://ferdelance.{os.environ.get("DOMAIN")}')

    project = ctx.load("58981bcbab77ef4b8e01207134c38873e0936a9ab88cd76b243a2e2c85390b94")

    clients = ctx.clients(project)

    client_id_1, client_id_2 = [c.client_id for c in clients]

    q = project.extract()

    q = q.add_plan(TrainTestSplit("MedHouseValDiscrete", 0.2))

    q = q.add_model(
        FederatedRandomForestClassifier(
            strategy=StrategyRandomForestClassifier.MERGE, parameters=ParametersRandomForestClassifier(n_estimators=10)
        )
    )

    a: Artifact = ctx.submit(project, q)

    print("Artifact id:", a.artifact_id)

    last_state = ""
    while (status := ctx.status(a)).status != "COMPLETED":
        if status.status == last_state:
            print(".", end="", flush=True)
        else:
            last_state = status.status
            print(last_state, end="", flush=True)
        time.sleep(0.5)
    print("done!")

    aggregated_model_path = ctx.get_model(a)

    print("model saved to:          ", aggregated_model_path)

    partial_model_path_1 = ctx.get_partial_model(a, client_id_1)

    print("partial model 1 saved to:", partial_model_path_1)

    partial_model_path_2 = ctx.get_partial_model(a, client_id_2)

    print("partial model 2 saved to:", partial_model_path_2)

    df = pd.read_csv("data/california_housing.validation.csv")

    cls_pa1 = FederatedRandomForestClassifier(load=partial_model_path_1)
    cls_pa2 = FederatedRandomForestClassifier(load=partial_model_path_2)
    cls_agg = FederatedRandomForestClassifier(load=aggregated_model_path)

    X = df.drop("MedHouseValDiscrete", axis=1).values
    Y = df["MedHouseValDiscrete"]

    print("Partial Model 1")
    evaluate(cls_pa1, X, Y)
    print()

    print("Partial Model 2")
    evaluate(cls_pa2, X, Y)
    print()

    print("Aggregated model")
    evaluate(cls_agg, X, Y)
    print()
