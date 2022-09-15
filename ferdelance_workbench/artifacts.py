from uuid import uuid4


class Artifact:

    def __init__(self) -> None:
        self.artifact_id = str(uuid4())
        self.filter = None
        self.model = None
        self.strategy = None

    def set_filter(self, filter) -> None:
        self.filter = filter

    def set_model(self, model) -> None:
        self.model = model

    def set_strategy(self, strategy) -> None:
        self.strategy = strategy
