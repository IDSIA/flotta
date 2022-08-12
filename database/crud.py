from sqlalchemy.orm import Session
from .tables import Client

import os
import logging


def create_user(db: Session, version: str, public_key: str) -> Client:
    logging.info(f'creating new user with version={version}')

    db_client = Client(version=version, public_key=public_key)

    db.add(db_client)
    db.commit()
    db.refresh(db_client)

    return db_client
