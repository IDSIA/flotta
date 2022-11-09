from ferdelance_shared.schemas import DataSource
from ferdelance_shared.exchange import Exchange

from .. import __version__
from .datasources import DataSourceFile, DataSourceDB

import logging
import os
import yaml


LOGGER = logging.getLogger(__name__)


class ConfigError(Exception):

    def __init__(self,  *args: str) -> None:
        self.what_is_missing: list[str] = list(args)


class Config:

    def __init__(self, server: str, workdir: str, heartbeat: float, datasources: list[dict[str, str]]) -> None:
        self.server: str = server.rstrip('/')
        self.workdir: str = workdir

        self.heartbeat: float = heartbeat

        self.client_id: str

        self.exc: Exchange

        self.datasources_list: list[dict[str, str]] = datasources
        self.datasources: dict[str, DataSourceFile | DataSourceDB] = dict()
        self.datasources_by_id: dict[str, DataSource] = dict()

        self.path_joined: str = os.path.join(self.workdir, '.joined')
        self.path_properties: str = os.path.join(self.workdir, 'properties.yaml')
        self.path_server_key: str = os.path.join(self.workdir, 'server_key.pub')
        self.path_private_key: str = os.path.join(self.workdir, 'private_key.pem')
        self.path_artifact_folder: str = os.path.join(self.workdir, 'artifacts')
        self.path_artifact_folder: str = os.path.join(self.workdir, 'artifacts')

    def check(self) -> None:
        # check for existing working directory
        if os.path.exists(self.workdir):
            LOGGER.info(f'loading properties from working directory {self.workdir}')

            # TODO: check how to enable permission check with docker
            # status = os.stat(self.workdir)
            # chmod = stat.S_IMODE(status.st_mode & 0o777)

            # if chmod != 0o700:
            #     LOGGER.error(f'working directory {self.workdir} has wrong permissions!')
            #     LOGGER.error(f'expected {0o700} found {chmod}')
            #     sys.exit(2)

            if os.path.exists(self.path_properties):
                # load properties
                LOGGER.info(f'loading properties file from {self.path_properties}')
                with open(self.path_properties, 'r') as f:
                    props = yaml.safe_load(f)

                    if not props:
                        raise ConfigError()

                    self.client_id = props['client_id']
                    self.server = props['server']

                    self.exc.set_token(props['client_token'])

                    if self.heartbeat == None:
                        self.heartbeat = props['heartbeat']
                    if self.heartbeat == None:
                        self.heartbeat = 1.0

                    # TODO: load data sources

                    self.path_joined = props['path_joined']
                    self.path_server_key = props['path_server_key']
                    self.path_private_key = props['path_private_key']

            if os.path.exists(self.path_private_key):
                LOGGER.info(f'private key found at {self.path_private_key}')
                self.exc.load_key(self.path_private_key)
            else:
                LOGGER.info(f'private key not found at {self.path_private_key}')
                raise ConfigError('pk', 'join')

            if os.path.exists(self.path_joined):
                # already joined

                if os.path.exists(self.path_server_key):
                    LOGGER.info(f'reading server key from {self.path_server_key}')
                    self.exc.load_remote_key(self.path_server_key)

                else:
                    LOGGER.info(f'reading server key not found at {self.path_server_key}')
                    raise ConfigError('join')

                if self.client_id is None or self.exc.token is None or not os.path.exists(self.path_joined):
                    LOGGER.info(f'client not joined')
                    raise ConfigError('join')

            else:
                LOGGER.info(f'client not joined')
                raise ConfigError('join')

        else:
            # empty directory
            LOGGER.info('working directory does not exists')
            raise ConfigError('wd', 'pk', 'join')

    def dump(self):
        """Save current configuration to a file in the working directory."""
        with open(self.path_properties, 'w') as f:
            yaml.safe_dump({
                'version': __version__,

                'server': self.server,
                'workdir': self.workdir,
                'heartbeat': self.heartbeat,

                'datasources': self.datasources_list,

                'client_id': self.client_id,
                'client_token': self.exc.token,

                'path_joined': self.path_joined,
                'path_server_key': self.path_server_key,
                'path_private_key': self.path_private_key,
            }, f)
