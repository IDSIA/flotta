from cryptography.fernet import Fernet
from sqlalchemy.orm import Session
from pytimeparse import parse

from .tables import Setting

import base64
import os


KEY_TOKEN_EXPIRATION = 'TOKEN_EXPIRATION'


def setup_settings(db: Session) -> None:

    TOKEN_EXPIRATION = os.environ.get('TOKEN_EXPIRATION', str(parse('90 day')))

    kvs = KeyValueStore(db)
    kvs.put_str(KEY_TOKEN_EXPIRATION, TOKEN_EXPIRATION)


def build_settings_cipher() -> Fernet:
    smp: str | None = os.environ.get('SERVER_MAIN_PASSWORD')

    assert smp is not None

    smp0 = smp[:32].encode('utf-8')
    smp1 = smp[-32:].encode('utf-8')
    smp2 = base64.b64encode(bytes(a ^ b for a, b in zip(smp0, smp1)))

    return Fernet(smp2)


class KeyValueStore:

    def __init__(self, db: Session) -> None:
        self.db = db
        self.cipher = build_settings_cipher()

    def put_bytes(self, key: str, value_in: bytes) -> None:
        value = self.cipher.encrypt(value_in)
        value = base64.b64encode(value)
        value = value.decode('utf8')

        # check that entry exists
        db_setting = self.db.query(Setting).filter(Setting.key == key).first()

        if db_setting is None:
            db_setting = Setting(key=key, value=value)
            self.db.add(db_setting)
        else:
            self.db.query(Setting).filter(Setting.key == key).update({Setting.value: value})

        self.db.commit()
        self.db.refresh(db_setting)

    def put_str(self, key: str, value_in: str) -> None:
        value = value_in.encode('utf8')
        self.put_bytes(key, value)

    def put_int(self, key: str, value_in: int) -> None:
        value = str(value_in)
        self.put_str(key, value)

    def put_float(self, key: str, value_in: float) -> None:
        value = str(value_in)
        self.put_str(key, value)

    def get_bytes(self, key: str) -> bytes:
        db_setting: Setting | None = self.db.query(Setting).filter(Setting.key == key).one_or_none()

        if db_setting is None:
            raise ValueError(f'Nothing found in settings for key={key}')

        value = db_setting.value
        value = value.encode('utf8')
        value = base64.b64decode(value)
        value = self.cipher.decrypt(value)

        return value

    def get_str(self, key: str) -> str:
        value = self.get_bytes(key)
        value = value.decode('utf8')

        return value

    def get_int(self, key: str) -> int:
        value = self.get_str(key)
        value = int(value)

        return value

    def get_float(self, key: str) -> float:
        value = self.get_str(key)
        value = float(value)

        return value
