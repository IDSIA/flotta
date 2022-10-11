from pydantic import BaseModel


class Metrics(BaseModel):
    accuracy_score: float = -1.0
    precision_score: float = -1.0
    recall_score: float = -1.0
    f1_score: float = -1.0
    auc_score: float = -1.0
    loss: float = -1.0

    confusion_matrix_TP: float = -1.0
    confusion_matrix_FN: float = -1.0
    confusion_matrix_FP: float = -1.0
    confusion_matrix_TN: float = -1.0
