from typing import Any

from ferdelance.config import config_manager
from ferdelance.database.repositories.core import Repository, AsyncSession
from ferdelance.database.tables import Setting

from cryptography.fernet import Fernet
from sqlalchemy import select, update

import base64


KEY_CLIENT_TOKEN_EXPIRATION = "TOKEN_CLIENT_EXPIRATION"
KEY_USER_TOKEN_EXPIRATION = "TOKEN_USER_EXPIRATION"


async def setup_settings(session: AsyncSession) -> None:
    conf = config_manager.get().server
    CLIENT_TOKEN_EXPIRATION = conf.token_client_expiration
    USER_TOKEN_EXPIRATION = conf.token_user_expiration

    kvs = KeyValueStore(session)
    await kvs.put_any(KEY_CLIENT_TOKEN_EXPIRATION, CLIENT_TOKEN_EXPIRATION)

    kvs = KeyValueStore(session)
    await kvs.put_any(KEY_USER_TOKEN_EXPIRATION, USER_TOKEN_EXPIRATION)


def build_settings_cipher() -> Fernet:
    conf = config_manager.get().server

    server_main_password: str | None = conf.main_password

    assert server_main_password is not None

    smp0 = server_main_password[:32].encode("utf8")
    smp1 = server_main_password[-32:].encode("utf8")
    smp2 = base64.b64encode(bytes(a ^ b for a, b in zip(smp0, smp1)))

    return Fernet(smp2)


class KeyValueStore(Repository):
    """This is an encrypted storage for data required by the server."""

    def __init__(self, session: AsyncSession, encode: str = "utf8") -> None:
        super().__init__(session)
        self.cipher = build_settings_cipher()
        self.encode: str = encode

    async def put_bytes(self, key: str, value_in: bytes) -> None:
        value = self.cipher.encrypt(value_in)
        value = base64.b64encode(value)
        value = value.decode(self.encode)

        # check that entry exists
        res = await self.session.execute(select(Setting).where(Setting.key == key).limit(1))
        db_setting: Setting | None = res.scalar_one_or_none()

        if db_setting is None:
            db_setting = Setting(key=key, value=value)
            self.session.add(db_setting)
        else:
            await self.session.execute(update(Setting).where(Setting.key == key).values(value=value))

        await self.session.commit()

    async def put_str(self, key: str, value_in: str) -> None:
        value = value_in.encode(self.encode)
        await self.put_bytes(key, value)

    async def put_int(self, key: str, value_in: int) -> None:
        value = str(value_in)
        await self.put_str(key, value)

    async def put_float(self, key: str, value_in: float) -> None:
        value = str(value_in)
        await self.put_str(key, value)

    async def put_any(self, key: str, value_in: Any) -> None:
        value = str(value_in)
        await self.put_str(key, value)

    async def get_bytes(self, key: str) -> bytes:
        """Can raise NoResultFound"""
        res = await self.session.execute(select(Setting).where(Setting.key == key))

        db_setting: Setting = res.scalar_one()

        value = db_setting.value
        value = value.encode(self.encode)
        value = base64.b64decode(value)
        value = self.cipher.decrypt(value)

        return value

    async def get_str(self, key: str) -> str:
        """Can raise NoResultFound"""
        value = await self.get_bytes(key)
        value = value.decode(self.encode)

        return value

    async def get_int(self, key: str) -> int:
        """Can raise NoResultFound"""
        value = await self.get_str(key)
        value = int(value)

        return value

    async def get_float(self, key: str) -> float:
        """Can raise NoResultFound"""
        value = await self.get_str(key)
        value = float(value)

        return value
