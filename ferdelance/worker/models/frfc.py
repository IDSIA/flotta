from .core import Aggregator

from ferdelance_shared.schemas.models import StrategyRandomForestClassifier

from sklearn.ensemble import RandomForestClassifier, VotingClassifier
from sklearn.preprocessing import LabelEncoder

import logging

LOGGER = logging.getLogger(__name__)


class AggregatorRandomForestClassifier(Aggregator):

    def aggregate(self, strategy: StrategyRandomForestClassifier, model_a: RandomForestClassifier | VotingClassifier, model_b: RandomForestClassifier):
        LOGGER.info(f'AggregatorRandomForestClassifier: using strategy={strategy}')

        if strategy == StrategyRandomForestClassifier.MERGE:
            return self.merge(model_a, model_b)

        elif strategy == StrategyRandomForestClassifier.MAJORITY_VOTE:
            return self.majority_vote()

        else:
            raise ValueError(f'Unsupported strategy: {strategy}')

    def merge(self, model_a: RandomForestClassifier, model_b: RandomForestClassifier) -> RandomForestClassifier:
        """Solution adapted from: https://stackoverflow.com/a/28508619/1419058
        """
        model_a.estimators_ += model_b.estimators_
        model_a.n_estimators = len(model_a.estimators_)

        return model_a

    def majority_vote(self, model_a: RandomForestClassifier | VotingClassifier, model_b: RandomForestClassifier) -> VotingClassifier:
        """Solution adapted from: https://stackoverflow.com/a/54610569/1419058
        """

        if isinstance(model_a, VotingClassifier):
            model_a.estimators_ += (f'{len(model_a.estimators_)}', model_b)
            return model_a

        models = [('0', model_a), ('1', model_b)]
        vc = VotingClassifier(
            estimators=models,
            voting='soft'
        )

        vc.estimators_ = models
        vc.le_ = LabelEncoder().fit(model_a.classes_)  # TODO: check if this is valid?
        vc.classes_ = vc.le_.classes_

        return vc
