__all__ = [
    'model_creator',
    'LocalFederatedRandomForestClassifier',
]

from ferdelance_shared.schemas.models import Model
from ferdelance_shared.schemas.models.core import GenericModel

from .random_forest import LocalFederatedRandomForestClassifier


def model_creator(model: Model) -> GenericModel:
    if model.name == LocalFederatedRandomForestClassifier.name:
        return LocalFederatedRandomForestClassifier.from_model(model)

    raise ValueError(f'model={model.name} not supported')
