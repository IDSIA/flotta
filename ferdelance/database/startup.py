from .tables import Client, Setting

from sqlalchemy.orm import Session

import logging

LOGGER = logging.getLogger(__name__)

def init_content(db: Session) -> None:
    """Initialize all tables in the database.

    :param db:
        Session with the connection to the database.
    """

    logging.info('Database creation started')

    engine = db.get_bind()

    Client.__table__.create(bind=engine, checkfirst=True)
    Setting.__table__.create(bind=engine, checkfirst=True)

    db.commit()

    LOGGER.info('Database creation completed')
