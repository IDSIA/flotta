from ferdelance import __version__
from ferdelance.client.config import Config, ConfigError
from ferdelance.client.exceptions import RelaunchClient, ErrorClient, UpdateClient
from ferdelance.client.services.scheduling import ScheduleActionService
from ferdelance.client.services.routes import RouteService
from ferdelance.shared.actions import Action
from ferdelance.schemas.node import JoinData, JoinRequest
from ferdelance.shared.exchange import Exchange

from time import sleep

import logging
import os
import requests


LOGGER = logging.getLogger(__name__)


class FerdelanceClient:
    def __init__(self, config: Config) -> None:
        # possible states are: work, exit, update, install
        self.status: Action = Action.INIT

        self.config: Config = config

        self.setup_completed: bool = False
        self.stop: bool = False

    def check_server(self):
        routes_service = RouteService(self.config)
        routes_service.check()

    def beat(self):
        LOGGER.debug(f"waiting for {self.config.heartbeat}")
        sleep(self.config.heartbeat)

    def setup(self) -> None:
        """Component initialization (keys setup), joining the server (if not already joined), and sending metadata."""

        LOGGER.info("client initialization")

        # create required directories
        os.makedirs(self.config.workdir, exist_ok=True)
        os.chmod(self.config.workdir, 0o700)
        os.makedirs(self.config.data.path_artifacts_folder(), exist_ok=True)
        os.chmod(self.config.data.path_artifacts_folder(), 0o700)

        exc = Exchange()

        if self.config.private_key_location is None:
            if os.path.exists(self.config.path_private_key()):
                # use existing one
                LOGGER.info("private key location not set: using existing one")
                self.config.private_key_location = self.config.path_private_key()

            else:
                # generate new key
                LOGGER.info("private key location not set: creating a new one")

                exc.generate_key()
                exc.save_private_key(self.config.path_private_key())

        elif not os.path.exists(self.config.private_key_location):
            LOGGER.info("private key location not found: creating a new one")

            exc.generate_key()
            exc.save_private_key(self.config.path_private_key())

        else:
            # load key
            LOGGER.info(f"private key found at {self.config.private_key_location}")
            exc.load_key(self.config.private_key_location)

        self.check_server()

        if os.path.exists(self.config.path_properties()):
            # already joined
            LOGGER.info(f"loading connection data from {self.config.path_properties()}")
            self.config.read_props()

        else:
            # not joined yet
            LOGGER.info("collecting system info")

            join_data = JoinRequest(
                name=self.config.name,
                system=self.config.machine_system,
                mac_address=self.config.machine_mac_address,
                node=self.config.machine_node,
                public_key=exc.transfer_public_key(),
                version=__version__,
            )

            try:
                routes_service: RouteService = RouteService(self.config)
                data: JoinData = routes_service.join(join_data)
                self.config.join(data.id, data.token, data.public_key)

            except requests.HTTPError as e:
                if e.response.status_code == 404:
                    LOGGER.error(f"remote server {self.config.server} not found.")
                    self.beat()
                    raise RelaunchClient()

                if e.response.status_code == 403:
                    LOGGER.error("wrong local files, maybe the client has been removed?")
                    raise ErrorClient()

                LOGGER.exception(e)
                raise ErrorClient()

            except requests.exceptions.RequestException as e:
                LOGGER.error("connection refused")
                LOGGER.exception(e)
                self.beat()
                raise RelaunchClient()

            except Exception as e:
                LOGGER.error("internal error")
                LOGGER.exception(e)
                raise RelaunchClient()

        LOGGER.info("setup completed")
        self.setup_completed = True

    def stop_loop(self):
        LOGGER.info("gracefully stopping application")
        self.stop = True

    def run(self) -> int:
        """Main loop where the client contact the server for updates.

        :return:
            Exit code to use
        """

        try:
            LOGGER.info("running client")

            if not self.setup_completed:
                self.setup()

            routes_service = RouteService(self.config)

            if self.config.leave:
                routes_service.leave()

            routes_service.send_metadata()

            scheduler = ScheduleActionService(self.config)

            while self.status != Action.CLIENT_EXIT and not self.stop:
                try:
                    LOGGER.debug("requesting update")

                    action, data = routes_service.get_update({})

                    LOGGER.debug(f"update: action={action}")

                    self.status = scheduler.schedule(action, data)

                    if self.status == Action.CLIENT_UPDATE:
                        raise UpdateClient()

                except UpdateClient as e:
                    raise e

                except ValueError as e:
                    # TODO: discriminate between bad and acceptable exceptions
                    LOGGER.exception(e)

                except requests.HTTPError as e:
                    LOGGER.exception(e)
                    # TODO what to do in this case?

                except requests.exceptions.RequestException as e:
                    LOGGER.error("connection refused")
                    LOGGER.exception(e)
                    # TODO what to do in this case?

                except Exception as e:
                    LOGGER.error("internal error")
                    LOGGER.exception(e)

                    # TODO what to do in this case?
                    raise ErrorClient()

                self.beat()

        except UpdateClient:
            LOGGER.info("update application and dependencies")
            return 1

        except ConfigError as e:
            LOGGER.error("could not complete setup")
            LOGGER.exception(e)
            raise ErrorClient()

        except KeyboardInterrupt:
            LOGGER.info("stopping client")
            self.stop = True

        except Exception as e:
            LOGGER.error("Unknown error")
            LOGGER.exception(e)
            raise ErrorClient()

        if self.stop:
            raise ErrorClient()

        return 0
