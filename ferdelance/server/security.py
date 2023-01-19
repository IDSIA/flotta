from ferdelance.config import conf
from ferdelance.database import get_session, AsyncSession
from ferdelance.database.const import MAIN_KEY, PRIVATE_KEY, PUBLIC_KEY
from ferdelance.database.data import TYPE_CLIENT
from ferdelance.database.services import KeyValueStore, ComponentService
from ferdelance.database.schemas import Client, Component, Token
from ferdelance.shared.exchange import Exchange

from fastapi import Depends, HTTPException
from fastapi.security import HTTPBasicCredentials, HTTPBearer
from sqlalchemy.exc import NoResultFound

from datetime import timedelta, datetime

import logging

LOGGER = logging.getLogger(__name__)


async def generate_keys(session: AsyncSession) -> Exchange:
    """Initialization method for the generation of keys for the server.
    Requires to have the environment variable 'SERVER_MAIN_PASSWORD'.

    :param db:
        Current session to the database.
    """

    SMP_VALUE = conf.SERVER_MAIN_PASSWORD

    if SMP_VALUE is None:
        LOGGER.critical(f"Environment variable {MAIN_KEY} is missing.")
        raise ValueError(f"{MAIN_KEY} missing")

    kvs = KeyValueStore(session)
    e = Exchange()

    try:
        db_smp_key = await kvs.get_str(MAIN_KEY)

        if db_smp_key != SMP_VALUE:
            LOGGER.fatal(f"Environment variable {MAIN_KEY} invalid: please set the correct password!")
            raise Exception(f"{MAIN_KEY} invalid")

    except NoResultFound:
        await kvs.put_str(MAIN_KEY, SMP_VALUE)
        LOGGER.info(f"Application initialization, Environment variable {MAIN_KEY} saved in storage")

    try:
        pk = await kvs.get_bytes(PRIVATE_KEY)
        LOGGER.info("Keys are already available")
        e.set_key_bytes(pk)

    except NoResultFound:
        # generate new keys
        LOGGER.info("Keys generation started")

        e.generate_key()

        private_bytes: bytes = e.get_private_key_bytes()
        public_bytes: bytes = e.get_public_key_bytes()

        await kvs.put_bytes(PRIVATE_KEY, private_bytes)
        await kvs.put_bytes(PUBLIC_KEY, public_bytes)

        e.set_key_bytes(private_bytes)

        LOGGER.info("Keys generation completed")

    return e


async def check_token(
    credentials: HTTPBasicCredentials = Depends(HTTPBearer()), session: AsyncSession = Depends(get_session)
) -> Component | Client:
    """Checks if the given token exists in the database.

    :param credentials:
        Content of Authorization header.
    :session:
        Session on the database.
    :return:
        The component object associated with the authorization header, otherwise an exception is raised.
    """
    given_token: str = credentials.credentials  # type: ignore

    cs: ComponentService = ComponentService(session)

    try:
        token: Token = await cs.get_token_by_token(given_token)

    except NoResultFound:
        LOGGER.warning("received token does not exist in database")
        raise HTTPException(401, "Invalid access token")

    # TODO: add expiration to token, and also an endpoint to update the token using an expired one

    component_id = str(token.component_id)

    if not token.valid:
        LOGGER.warning("received invalid token")
        raise HTTPException(403, "Permission denied")

    if token.creation_time + timedelta(seconds=token.expiration_time) < datetime.now(token.creation_time.tzinfo):
        LOGGER.warning(f"component_id={component_id}: received expired token: invalidating")
        await cs.invalidate_tokens(component_id)
        # allow access only for a single time, since the token update has priority

    LOGGER.debug(f"component_id={component_id}: received valid token")

    try:
        component: Component | Client = await cs.get_by_id(component_id)

        if component.left or not component.active:
            LOGGER.warning("Client that left or has been deactivated tried to connect!")
            raise HTTPException(403, "Permission denied")

        return component

    except NoResultFound:
        LOGGER.warning(f"valid token does not have client!")
        raise HTTPException(403, "Permission denied")
