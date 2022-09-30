from .core import DBSessionService, Session

from ..tables import ClientApp


class ClientAppService(DBSessionService):

    def __init__(self, db: Session) -> None:
        super().__init__(db)

    def get_newest_app_version(self) -> ClientApp:
        db_client_app: ClientApp = self.db.query(ClientApp)\
            .filter(ClientApp.active)\
            .order_by(ClientApp.creation_time.desc())\
            .first()

        if db_client_app is None:
            return None

        return db_client_app

    def get_newest_app(self) -> ClientApp:
        return self.db.query(ClientApp)\
            .filter(ClientApp.active)\
            .order_by(ClientApp.creation_time.desc())\
            .first()
