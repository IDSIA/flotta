from .core import DBSessionService, AsyncSession
from ..tables import Setting

from cryptography.fernet import Fernet
from pytimeparse import parse
from sqlalchemy import select

import base64
import os


KEY_TOKEN_EXPIRATION = 'TOKEN_EXPIRATION'


async def setup_settings(session: AsyncSession) -> None:

    TOKEN_EXPIRATION = os.environ.get('TOKEN_EXPIRATION', str(parse('90 day')))

    kvs = KeyValueStore(session)
    await kvs.put_str(KEY_TOKEN_EXPIRATION, TOKEN_EXPIRATION)


def build_settings_cipher() -> Fernet:
    smp: str | None = os.environ.get('SERVER_MAIN_PASSWORD')

    assert smp is not None

    smp0 = smp[:32].encode('utf-8')
    smp1 = smp[-32:].encode('utf-8')
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
            select(Setting).filter(Setting.key == key).limit(1)
        )
        db_setting = res.scalar_one_or_none()

        if db_setting is None:
            db_setting = Setting(key=key, value=value)
            self.session.add(db_setting)
        else:
            db_setting.value = value

        await self.session.commit()
        await self.session.refresh(db_setting)

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
            .filter(Setting.key == key)
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
