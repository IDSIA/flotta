from ferdelance.config import Configuration
from ferdelance.client.state import State
from ferdelance.client.exceptions import ConfigError, RelaunchClient, ErrorClient, UpdateClient
from ferdelance.client.services.scheduling import ScheduleActionService
from ferdelance.client.services.routes import RouteService
from ferdelance.logging import get_logger
from ferdelance.shared.actions import Action
from ferdelance.shared.exchange import Exchange

from time import sleep

import ray

import os
import requests


LOGGER = get_logger(__name__)


def start_client(config: Configuration, leave: bool = False):
    LOGGER.info("Starting a new client")
    state = State(config, leave)
    client = FerdelanceClient.remote(state)
    h = client.run.remote()  # type: ignore
    return ray.get([h])


@ray.remote
class FerdelanceClient:
    def __init__(self, state: State) -> None:
        # possible states are: work, exit, update, install
        self.status: Action = Action.INIT

        self.state: State = state

        self.setup_completed: bool = False
        self.stop: bool = False

    def _check_node(self):
        routes_service = RouteService(self.state)
        routes_service.check()

    def _beat(self):
        LOGGER.debug(f"waiting for {self.state.heartbeat}")
        sleep(self.state.heartbeat)

    def _setup(self) -> None:
        """Component initialization (keys setup), joining the server node (if not already joined), and sending
        metadata.
        """

        LOGGER.info("client initialization")

        # create required directories
        os.makedirs(self.state.workdir, exist_ok=True)
        os.chmod(self.state.workdir, 0o700)
        os.makedirs(self.state.config.storage_artifact_dir(), exist_ok=True)
        os.chmod(self.state.config.storage_artifact_dir(), 0o700)

        exc = Exchange()

        if self.state.private_key_location is None:
            if os.path.exists(self.state.path_private_key()):
                # use existing one
                LOGGER.info("private key location not set: using existing one")
                self.state.private_key_location = self.state.path_private_key()

            else:
                # generate new key
                LOGGER.info("private key location not set: creating a new one")

                exc.generate_key()
                exc.save_private_key(self.state.path_private_key())

        elif not os.path.exists(self.state.private_key_location):
            LOGGER.info("private key location not found: creating a new one")

            exc.generate_key()
            exc.save_private_key(self.state.path_private_key())

        else:
            # load key
            LOGGER.info(f"private key found at {self.state.private_key_location}")
            exc.load_key(self.state.private_key_location)

        self._check_node()

        if os.path.exists(self.state.path_properties()):
            # already joined
            LOGGER.info(f"loading connection data from {self.state.path_properties()}")
            self.state.read_props()

        else:
            # not joined yet
            LOGGER.info("collecting system info")

            try:
                routes_service: RouteService = RouteService(self.state)
                routes_service.join()

            except requests.HTTPError as e:
                if e.response.status_code == 404:
                    LOGGER.error(f"remote server node {self.state.server} not found.")
                    self._beat()
                    raise RelaunchClient()

                if e.response.status_code == 403:
                    LOGGER.error("wrong local files, maybe the client has been removed?")
                    raise ErrorClient()

                LOGGER.exception(e)
                raise ErrorClient()

            except requests.exceptions.RequestException as e:
                LOGGER.error("connection refused")
                LOGGER.exception(e)
                self._beat()
                raise RelaunchClient()

            except Exception as e:
                LOGGER.error("internal error")
                LOGGER.exception(e)
                raise RelaunchClient()

        LOGGER.info("setup completed")
        self.setup_completed = True

    def _stop_loop(self):
        LOGGER.info("gracefully stopping application")
        self.stop = True

    def run(self) -> int:
        """Main loop where the client contact the server node for updates.

        :return:
            Exit code to use
        """

        try:
            LOGGER.info("running client")

            if not self.setup_completed:
                self._setup()

            routes_service = RouteService(self.state)

            if self.state.leave:
                routes_service.leave()

            routes_service.send_metadata()

            scheduler = ScheduleActionService(self.state)

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

                self._beat()

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
