__all__ = [
    'model_creator',
]

from ferdelance_shared.models import Model
from ferdelance_shared.models.core import GenericModel
from ferdelance_shared.models.FederatedRandomForestClassifier import FederatedRandomForestClassifier


def model_creator(model: Model) -> GenericModel:
    if model.name == FederatedRandomForestClassifier.name:
        return FederatedRandomForestClassifier.from_model(model)

    raise ValueError(f'model={model.name} not supported')
