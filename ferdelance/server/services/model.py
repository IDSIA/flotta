from . import DBSessionService, Session

from ...database.tables import Model


class ModelService(DBSessionService):

    def __init__(self, db: Session) -> None:
        super().__init__(db)

    def get_model_by_id(self, model_id: int) -> Model | None:
        return self.db.query(Model).filter(Model.model_id == model_id).first()
