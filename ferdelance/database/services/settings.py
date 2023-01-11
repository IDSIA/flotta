from .core import DBSessionService, AsyncSession
from ..tables import Setting
from ...config import conf

from cryptography.fernet import Fernet
from pytimeparse import parse
from sqlalchemy import select, update

import base64
import os


KEY_CLIENT_TOKEN_EXPIRATION = 'TOKEN_CLIENT_EXPIRATION'
KEY_USER_TOKEN_EXPIRATION = 'TOKEN_USER_EXPIRATION'


async def setup_settings(session: AsyncSession) -> None:

    CLIENT_TOKEN_EXPIRATION = os.environ.get(KEY_CLIENT_TOKEN_EXPIRATION, str(parse('90 day')))
    USER_TOKEN_EXPIRATION = os.environ.get(KEY_USER_TOKEN_EXPIRATION, str(parse('30 day')))

    kvs = KeyValueStore(session)
    await kvs.put_str(KEY_CLIENT_TOKEN_EXPIRATION, CLIENT_TOKEN_EXPIRATION)

    kvs = KeyValueStore(session)
    await kvs.put_str(KEY_USER_TOKEN_EXPIRATION, USER_TOKEN_EXPIRATION)


def build_settings_cipher() -> Fernet:
    server_main_password: str | None = conf.SERVER_MAIN_PASSWORD

    assert server_main_password is not None

    smp0 = server_main_password[:32].encode('utf-8')
    smp1 = server_main_password[-32:].encode('utf-8')
    smp2 = base64.b64encode(bytes(a ^ b for a, b in zip(smp0, smp1)))

    return Fernet(smp2)


class KeyValueStore(DBSessionService):

    def __init__(self, session: AsyncSession) -> None:
        super().__init__(session)
        self.cipher = build_settings_cipher()

    async def put_bytes(self, key: str, value_in: bytes) -> None:
        value = self.cipher.encrypt(value_in)
        value = base64.b64encode(value)
        value = value.decode('utf8')

        # check that entry exists
        res = await self.session.execute(
            select(Setting).where(Setting.key == key).limit(1)
        )
        db_setting: Setting | None = res.scalar_one_or_none()

        if db_setting is None:
            db_setting = Setting(key=key, value=value)
            self.session.add(db_setting)
        else:
            await self.session.execute(
                update(Setting)
                .where(Setting.key == key)
                .values(value=value)
            )

        await self.session.commit()

    async def put_str(self, key: str, value_in: str) -> None:
        value = value_in.encode('utf8')
        await self.put_bytes(key, value)

    async def put_int(self, key: str, value_in: int) -> None:
        value = str(value_in)
        await self.put_str(key, value)

    async def put_float(self, key: str, value_in: float) -> None:
        value = str(value_in)
        await self.put_str(key, value)

    async def get_bytes(self, key: str) -> bytes:
        res = await self.session.execute(
            select(Setting)
            .where(Setting.key == key)
        )

        db_setting: Setting | None = res.scalar_one_or_none()

        if db_setting is None:
            raise ValueError(f'Nothing found in settings for key={key}')

        value = db_setting.value
        value = value.encode('utf8')
        value = base64.b64decode(value)
        value = self.cipher.decrypt(value)

        return value

    async def get_str(self, key: str) -> str:
        value = await self.get_bytes(key)
        value = value.decode('utf8')

        return value

    async def get_int(self, key: str) -> int:
        value = await self.get_str(key)
        value = int(value)

        return value

    async def get_float(self, key: str) -> float:
        value = await self.get_str(key)
        value = float(value)

        return value
