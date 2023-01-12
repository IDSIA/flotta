from ferdelance import __version__
from ferdelance.client.datasources import DataSourceDB, DataSourceFile
from ferdelance.client.config import Config, ConfigError
from ferdelance.client.services.actions import ActionService
from ferdelance.client.services.routes import RouteService
from ferdelance.shared.actions import Action
from ferdelance.shared.schemas import ClientJoinData

from getmac import get_mac_address
from time import sleep

import logging
import requests
import os
import platform
import sys
import uuid

LOGGER = logging.getLogger(__name__)


class FerdelanceClient:

    def __init__(self, server: str = 'http://localhost:8080', workdir: str = 'workdir', heartbeat: float = -1.0, leave: bool = False, datasources: list[dict[str, str]] = list(), machine_mac_addres: str | None = None, machine_node: str | None = None) -> None:
        # possible states are: work, exit, update, install
        self.status: Action = Action.INIT

        self.config: Config = Config(
            server=server,
            workdir=workdir,
            heartbeat=heartbeat,
            datasources=datasources,
        )

        self.flag_leave: bool = leave
        self.setup_completed: bool = False
        self.stop: bool = False

        self.machine_mac_address = machine_mac_addres
        self.machine_node = machine_node

    def beat(self):
        LOGGER.debug(f'waiting for {self.config.heartbeat}')
        sleep(self.config.heartbeat)

    def get_datasource(self, name: str, filter: str | None = None) -> None:
        # TODO
        pass

    def setup(self) -> None:
        """Client initialization (keys setup), joining the server (if not already joined), and sending metadata and sources available."""
        LOGGER.info('client initialization')

        try:
            self.config.check()

        except ConfigError as e:
            LOGGER.info(f'missing setup files: {e.what_is_missing}')

            for item in e.what_is_missing:

                if item == 'wd':
                    LOGGER.info(f'creating working directory={self.config.workdir}')
                    os.makedirs(self.config.workdir, exist_ok=True)
                    os.chmod(self.config.workdir, 0o700)
                    os.makedirs(self.config.path_artifact_folder, exist_ok=True)

                if item == 'pk':
                    LOGGER.info('private key does not exist: creating a new one')
                    self.config.exc.generate_key()
                    self.config.exc.save_private_key(self.config.path_private_key)

                if item == 'join':
                    routes_service = RouteService(self.config)
                    LOGGER.info('collecting system info')

                    machine_system: str = platform.system()

                    machine_mac_address: str = self.machine_mac_address or get_mac_address() or ''
                    machine_node: str = self.machine_node or str(uuid.getnode())

                    LOGGER.info(f'system info: machine_system={machine_system}')
                    LOGGER.info(f'system info: machine_mac_address={machine_mac_address}')
                    LOGGER.info(f'system info: machine_node={machine_node}')
                    LOGGER.info(f'client info: version={__version__}')

                    try:
                        data: ClientJoinData = routes_service.join(
                            system=machine_system,
                            mac_address=machine_mac_address,
                            node=machine_node,
                            encoded_public_key=self.config.exc.transfer_public_key(),
                            version=__version__
                        )

                        LOGGER.info('client join successful')

                        self.config.client_id = data.id
                        self.config.exc.set_token(data.token)
                        self.config.exc.set_remote_key(data.public_key)

                        self.config.exc.save_remote_key(self.config.path_server_key)

                        open(self.config.path_joined, 'a').close()
                    except requests.HTTPError as e:

                        if e.response.status_code == 404:
                            LOGGER.error(f'remote server {self.config.server} not found.')
                            self.beat()
                            sys.exit(0)

                        if e.response.status_code == 403:
                            LOGGER.error('client already joined, but no local files found!?')
                            sys.exit(2)

                        LOGGER.exception(e)
                        sys.exit(2)

                    except requests.exceptions.RequestException as e:
                        LOGGER.error('connection refused')
                        LOGGER.exception(e)
                        self.beat()
                        sys.exit(0)

                    except Exception as e:
                        LOGGER.error('internal error')
                        LOGGER.exception(e)
                        sys.exit(0)

        # setup local data sources
        for k, n, t, p in self.config.datasources_list:
            if k == 'file':
                self.config.datasources[n] = DataSourceFile(n, t, p)
            elif k == 'db':
                self.config.datasources[n] = DataSourceDB(n, t, p)
            else:
                LOGGER.error(f'Invalid data source: KIND={k} NAME={n} TYPE={t} CONN={p}')

        # save config locally
        self.config.dump()

        LOGGER.info('creating workdir folders')
        os.makedirs(self.config.path_artifact_folder, exist_ok=True)

        LOGGER.info('setup completed')
        self.setup_completed = True

    def stop_loop(self):
        LOGGER.info('stopping application')
        self.stop = True

    def run(self) -> int:
        """Main loop where the client contact the server for updates.

        :return:
            Exit code to use
        """
        action_service = ActionService(self.config)
        routes_service = RouteService(self.config)

        try:
            LOGGER.info('running client')

            if not self.setup_completed:
                self.setup()

            if self.flag_leave:
                routes_service.leave()

            routes_service.send_metadata()

            while self.status != Action.CLIENT_EXIT and not self.stop:
                try:
                    LOGGER.debug('requesting update')

                    action, data = routes_service.get_update({})

                    LOGGER.debug(f'update: action={action}')

                    # work loop
                    self.status = action_service.perform_action(action, data)

                    if self.status == Action.CLIENT_UPDATE:
                        LOGGER.info('update application and dependencies')
                        return 1

                except ValueError as e:
                    # TODO: discriminate between bad and acceptable exceptions
                    LOGGER.exception(e)

                except requests.HTTPError as e:
                    LOGGER.exception(e)
                    # TODO what to do in this case?

                except requests.exceptions.RequestException as e:
                    LOGGER.error('connection refused')
                    LOGGER.exception(e)
                    # TODO what to do in this case?

                except Exception as e:
                    LOGGER.error('internal error')
                    LOGGER.exception(e)

                    # TODO what to do in this case?
                    return 2

                self.beat()

        except ConfigError as e:
            LOGGER.error('could not complete setup')
            LOGGER.exception(e)
            return 2

        except Exception as e:
            LOGGER.error('Unknown error')
            LOGGER.exception(e)
            return 2

        if self.stop:
            return 2
