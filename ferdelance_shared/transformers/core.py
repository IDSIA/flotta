from ..artifacts.queries import QueryTransformer, QueryFeature


class GenericTransformer:
    name: str

    def save(self, path: str) -> None:
        raise NotImplementedError()

    def load(self, path: str) -> None:
        raise NotImplementedError()

    def aggregate(self) -> None:
        raise NotImplementedError()

    def fit(self) -> None:
        raise NotImplementedError()

    def transform(self) -> None:
        raise NotImplementedError()

    def build(self) -> QueryTransformer:
        raise NotImplementedError()


def convert_features_to_list(features: QueryFeature | list[QueryFeature] | str | list[str]) -> list[str]:
    if isinstance(features, str):
        features = [features]
    elif isinstance(features, QueryFeature):
        features = [features.feature_name]
    elif isinstance(features, list):
        f_list: list[str] = []
        for f in features:
            f_list.append(f.feature_name if isinstance(f, QueryFeature) else f)
        features = f_list
    return features
