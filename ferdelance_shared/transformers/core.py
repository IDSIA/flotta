from ..artifacts.queries import QueryTransformer


class GenericTransformer:

    def fit(self) -> None:
        raise NotImplementedError()

    def transform(self) -> None:
        raise NotImplementedError()

    def build(self) -> QueryTransformer:
        raise NotImplementedError()
