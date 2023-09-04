from ferdelance.config import config_manager
from ferdelance.logging import get_logger
from ferdelance.database import AsyncSession
from ferdelance.const import MAIN_KEY, PRIVATE_KEY, PUBLIC_KEY
from ferdelance.database.repositories import KeyValueStore
from ferdelance.shared.exchange import Exchange

from sqlalchemy.exc import NoResultFound


LOGGER = get_logger(__name__)


async def generate_keys(session: AsyncSession) -> Exchange:
    """Initialization method for the generation of keys for the server.
    Requires to have the environment variable 'SERVER_MAIN_PASSWORD'.

    :param db:
        Current session to the database.
    """

    SMP_VALUE = config_manager.get().node.main_password

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
