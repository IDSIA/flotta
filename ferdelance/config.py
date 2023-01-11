from sqlalchemy.engine import URL
from pydantic import BaseModel
from pytimeparse import parse

import os

cpu_count = os.cpu_count()


class Configuration(BaseModel):
    STANDALONE: bool = False
    STANDALONE_WORKERS: int = 1 if cpu_count is None else cpu_count - 1

    SERVER_MAIN_PASSWORD: str | None = os.environ.get('SERVER_MAIN_PASSWORD', None)

    DB_USER: str | None = os.environ.get('DB_USER', None)
    DB_PASS: str | None = os.environ.get('DB_PASS', None)

    DB_DIALECT: str = os.environ.get('DB_PROTOCOL', 'postgresql')
    DB_PORT: int = int(os.environ.get('DB_PORT', '5432'))
    DB_HOST: str | None = os.environ.get('DB_HOST', None)

    DB_SCHEMA: str = os.environ.get('DB_SCHEMA', 'ferdelance')

    DB_MEMORY: bool = False

    STORAGE_ARTIFACTS: str = str(os.path.join('.', 'storage', 'artifacts'))
    STORAGE_CLIENTS: str = str(os.path.join('.', 'storage', 'clients'))
    STORAGE_MODELS: str = str(os.path.join('.', 'storage', 'models'))

    FILE_CHUNK_SIZE: int = 4096

    CLIENT_TOKEN_EXPIRATION = os.environ.get('TOKEN_CLIENT_EXPIRATION', str(parse('90 day')))
    USER_TOKEN_EXPIRATION = os.environ.get('TOKEN_USER_EXPIRATION', str(parse('30 day')))

    def db_connection_url(self) -> str | None:
        if self.DB_MEMORY:
            return 'sqlite+aiosqlite://'

        dialect = self.DB_DIALECT.lower()

        assert self.DB_HOST is not None

        if dialect == 'sqlite':
            # in this case host is an absolute path
            return str(URL.create(f'sqlite+aiosqlite://{self.DB_HOST}'))

        if dialect == 'postgresql':
            assert self.DB_USER is not None
            assert self.DB_PASS is not None
            assert self.DB_PORT is not None

            return str(URL.create(
                'postgresql+asyncpg',
                self.DB_USER,
                self.DB_PASS,
                self.DB_HOST,
                self.DB_PORT,
                self.DB_HOST,
            ))

        raise ValueError(f'dialect {dialect} is not supported')


conf: Configuration = Configuration()

LOGGING_CONFIG = {
    'version': 1,
    'disable_existing_loggers': True,
    'formatters': {
        'standard': {
            'format': '%(asctime)s %(levelname)8s %(name)-48s:%(lineno)-3s %(message)s'
        }
    },
    'handlers': {
        'console': {
            'level': 'INFO',
            'class': 'logging.StreamHandler',
            'formatter': 'standard',
            'stream': 'ext://sys.stdout',
        },
        'console_critical': {
            'level': 'ERROR',
            'class': 'logging.StreamHandler',
            'formatter': 'standard',
            'stream': 'ext://sys.stdout',
        },
        'file': {
            'level': 'DEBUG',
            'class': 'logging.handlers.RotatingFileHandler',
            'formatter': 'standard',
            'filename': 'ferdelance.log',
            'maxBytes': 100000,
            'backupCount': 5,
        }
    },
    'loggers': {
        '': {
            'handlers': ['console'],
            'level': 'INFO',
            'propagate': False,
        },
        'uvicorn': {
            'handlers': ['console_critical', 'file'],
            'level': 'INFO',
            'propagate': False,
        },
    },
}
