from sqlalchemy.orm import Session


class DBSessionService:

    def __init__(self, db: Session) -> None:
        self.db: Session = db
