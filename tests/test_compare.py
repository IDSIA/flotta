import pandas as pd

from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import confusion_matrix, accuracy_score, roc_auc_score, f1_score

import os

local_dir = os.path.dirname(os.path.realpath(__file__))

if __name__ == "__main__":

    df1 = pd.read_csv(os.path.join(local_dir, "..", "data", "california_housing.MedInc1.csv"))
    df2 = pd.read_csv(os.path.join(local_dir, "..", "data", "california_housing.MedInc2.csv"))

    df = pd.concat([df1, df2])

    y_train = df["MedHouseValDiscrete"]
    x_train = df.drop("MedHouseValDiscrete", axis=1).values

    df_val = pd.read_csv(os.path.join(local_dir, "..", "data", "california_housing.validation.csv"))
    y_val = df_val["MedHouseValDiscrete"]
    x_val = df_val.drop("MedHouseValDiscrete", axis=1).values

    model = RandomForestClassifier(n_estimators=20)
    model.fit(x_train, y_train)

    # evaluate
    pred = model.predict(x_val)
    y_pred = (pred > 0.5).astype("int")

    f1 = f1_score(y_val, y_pred)
    ac = accuracy_score(y_val, y_pred)
    ra = roc_auc_score(y_val, pred)

    cm = confusion_matrix(y_val, pred)

    print("Accuracy:", ac)
    print("F1:      ", f1)
    print("ROC AUC: ", ra)

    print(cm)
