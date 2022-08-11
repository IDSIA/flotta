from .tables import Client

from sqlalchemy.orm import Session


def init_content(db: Session) -> None:
    """Initialize all tables in the database.

    :param db:
        Session with the connection to the database.
    """

    engine = db.get_bind()

    Client.__table__.create(bind=engine, checkfirst=True)
