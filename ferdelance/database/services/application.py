from .core import DBSessionService, Session

from ..tables import ClientApp


class ClientAppService(DBSessionService):

    def __init__(self, db: Session) -> None:
        super().__init__(db)

    def get_newest_app(self) -> ClientApp | None:
        return self.db.query(ClientApp)\
            .filter(ClientApp.active)\
            .order_by(ClientApp.creation_time.desc())\
            .one_or_none()
