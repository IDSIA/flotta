from .core import Aggregator

from ferdelance_shared.schemas.models import StrategyRandomForestClassifier

from sklearn.ensemble import RandomForestClassifier, VotingClassifier
from sklearn.preprocessing import LabelEncoder

import logging

LOGGER = logging.getLogger(__name__)


class AggregatorRandomForestClassifier(Aggregator):

    def aggregate(self, strategy: StrategyRandomForestClassifier, models: list[RandomForestClassifier]):
        LOGGER.info(f'AggregatorRandomForestClassifier: using strategy={strategy} with {len(models)} model(s)')

        if strategy == StrategyRandomForestClassifier.MERGE:
            return self.merge(models)

        elif strategy == StrategyRandomForestClassifier.MAJORITY_VOTE:
            return self.majority_vote()

        else:
            raise ValueError(f'Unsupported strategy: {strategy}')

    def merge(self, models: list[RandomForestClassifier]) -> RandomForestClassifier:
        """Solution adapted from: https://stackoverflow.com/a/28508619/1419058
        """
        base = models[0]

        for model in models[1:]:
            base.estimators_ += model.estimators_
            base.n_estimators = len(base.estimators_)

        return base

    def majority_vote(self, models: list[RandomForestClassifier]) -> VotingClassifier:
        """Solution adapted from: https://stackoverflow.com/a/54610569/1419058
        """

        estimators = [(f'{i}', m) for i, m in enumerate(models)]

        vc = VotingClassifier(estimators=estimators, voting='soft')

        vc.estimators_ = models
        vc.le_ = LabelEncoder().fit(models[0].classes_)  # TODO: check if this is valid?
        vc.classes_ = vc.le_.classes_

        return vc
