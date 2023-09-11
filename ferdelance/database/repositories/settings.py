from typing import Any

from ferdelance.config import config_manager
from ferdelance.database.repositories.core import Repository, AsyncSession
from ferdelance.database.tables import Setting
from ferdelance.shared.checksums import str_checksum

from cryptography.fernet import Fernet
from sqlalchemy import select, update

import base64


KEY_NODE_TOKEN_EXPIRATION = "TOKEN_CLIENT_EXPIRATION"
KEY_USER_TOKEN_EXPIRATION = "TOKEN_USER_EXPIRATION"


def build_settings_cipher() -> Fernet | None:
    conf = config_manager.get().node

    server_main_password: str = conf.main_password

    if not server_main_password:
        return None

    assert server_main_password is not None

    smp = base64.b64encode(str_checksum(server_main_password).encode())

    smp0 = smp[:32]
    smp1 = smp[-32:]
    smp2 = base64.b64encode(bytes(a ^ b for a, b in zip(smp0, smp1)))

    return Fernet(smp2)


class KeyValueStore(Repository):
    """This is an encrypted storage for data required by the server."""

    def __init__(self, session: AsyncSession, encode: str = "utf8") -> None:
        super().__init__(session)
        self.cipher: Fernet | None = build_settings_cipher()
        self.encode: str = encode

    async def put_bytes(self, key: str, value_in: bytes) -> None:
        if self.cipher:
            value: bytes = self.cipher.encrypt(value_in)
            value: bytes = base64.b64encode(value)
        else:
            value = value_in

        data: str = value.decode(self.encode)

        # check that entry exists
        res = await self.session.execute(select(Setting).where(Setting.key == key).limit(1))
        db_setting: Setting | None = res.scalar_one_or_none()

        if db_setting is None:
            db_setting = Setting(key=key, value=data)
            self.session.add(db_setting)
        else:
            await self.session.execute(update(Setting).where(Setting.key == key).values(value=data))

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

        data: str = db_setting.value
        value: bytes = data.encode(self.encode)
        if self.cipher:
            value: bytes = base64.b64decode(value)
            value: bytes = self.cipher.decrypt(value)

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
