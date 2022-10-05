from .. import __version__

from .tables import (
    Artifact,
    Client,
    ClientApp,
    ClientEvent,
    ClientToken,
    Job,
    Setting,
    Model,
    ClientDataSource,
    ClientFeature
)

from sqlalchemy.orm import Session

import logging

LOGGER = logging.getLogger(__name__)


def init_database(db: Session) -> None:
    """Initialize all tables in the database.

    :param db:
        Session with the connection to the database.
    """

    logging.info('Database creation started')

    engine = db.get_bind()

    Client.__table__.create(bind=engine, checkfirst=True)
    Setting.__table__.create(bind=engine, checkfirst=True)
    ClientToken.__table__.create(bind=engine, checkfirst=True)
    ClientEvent.__table__.create(bind=engine, checkfirst=True)
    ClientApp.__table__.create(bind=engine, checkfirst=True)
    Artifact.__table__.create(bind=engine, checkfirst=True)
    Job.__table__.create(bind=engine, checkfirst=True)
    Model.__table__.create(bind=engine, checkfirst=True)
    ClientDataSource.__table__.create(bind=engine, checkfirst=True)
    ClientFeature.__table__.create(bind=engine, checkfirst=True)

    db.commit()

    LOGGER.info('Database creation completed')
