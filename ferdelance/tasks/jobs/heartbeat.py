from ferdelance.config import config_manager, DataSourceConfiguration
from ferdelance.exceptions import ConfigError, ErrorClient, UpdateClient, InvalidAction
from ferdelance.logging import get_logger
from ferdelance.schemas.client import ClientUpdate
from ferdelance.schemas.updates import UpdateData
from ferdelance.security.exchange import Exchange
from ferdelance.shared.actions import Action
from ferdelance.tasks.jobs.execution import Execution

from pathlib import Path
from time import sleep

import json
import ray
import requests


LOGGER = get_logger(__name__)


@ray.remote
class Heartbeat:
    """Heartbeat is a continuous task launched by a node in client mode. This
    task will contact a regular intervals the reference scheduler node for new
    task to execute. Once a task has been found, it will be executed locally.
    """

    def __init__(self, client_id: str, remote_id: str, remote_public_key: str) -> None:
        # possible states are: work, exit, update, install
        self.status: Action = Action.INIT

        self.config = config_manager.get()
        self.leave = config_manager.leave()

        private_key_path: Path = config_manager.get().private_key_location()
        self.exc: Exchange = Exchange(private_key_path)
        self.exc.set_remote_key(remote_public_key)

        if self.config.join.url is None:
            raise ValueError("No remote server available")

        self.remote_id: str = remote_id
        self.remote_url: str = self.config.join.url
        self.remote_public_key: str = remote_public_key

        self.client_id: str = client_id
        self.stop: bool = False

    def _beat(self):
        LOGGER.debug(f"waiting for {self.config.node.heartbeat}")
        sleep(self.config.node.heartbeat)

    def _leave(self) -> None:
        """Send a leave request to the server."""

        headers, payload = self.exc.create(
            self.client_id,
            self.remote_id,
        )

        res = requests.post(
            f"{self.remote_url}/node/leave",
            headers=headers,
            data=payload,
        )

        res.raise_for_status()

        LOGGER.info(f"client left server {self.remote_url}")
        raise ErrorClient()

    def _update(self, content: ClientUpdate) -> UpdateData:
        """Heartbeat command to check for an update from the server."""
        LOGGER.debug("requesting update")

        headers, payload = self.exc.create(
            self.client_id,
            self.remote_id,
            content.json(),
        )

        res = requests.get(
            f"{self.remote_url}/client/update",
            headers=headers,
            data=payload,
        )

        res.raise_for_status()

        _, res_payload = self.exc.get_payload(res.content)

        return UpdateData(**json.loads(res_payload))

    def _start_execution(
        self,
        artifact_id: str,
        job_id: str,
    ) -> Action:
        dsc: list[DataSourceConfiguration] = self.config.datasources
        actor_handler = Execution.remote(
            self.client_id,
            artifact_id,
            job_id,
            self.remote_id,
            self.remote_url,
            self.remote_public_key,
            self.exc.transfer_private_key(),
            [d.dict() for d in dsc],
            False,
        )
        _ = actor_handler.run.remote()  # type: ignore

        return Action.DO_NOTHING

    def run(self) -> int:
        """Main loop where the client contact the server node for updates.

        :return:
            Exit code to use
        """

        try:
            LOGGER.info("running client")

            if self.leave:
                self._leave()
                return 0

            while self.status != Action.CLIENT_EXIT and not self.stop:
                try:
                    LOGGER.debug("requesting update")

                    update_data = self._update(ClientUpdate(action=self.status.name))

                    action = Action[update_data.action]

                    LOGGER.debug(f"update: action={action}")

                    # schedule action
                    if action == Action.EXECUTE:
                        LOGGER.info(
                            f"update: starting execution for artifact={update_data.artifact_id} job={update_data.job_id}"
                        )
                        self.status = self._start_execution(
                            update_data.artifact_id,
                            update_data.job_id,
                        )

                    elif action == Action.DO_NOTHING:
                        LOGGER.debug("nothing new from the server node")
                        self.status = Action.DO_NOTHING

                    elif self.status == Action.CLIENT_UPDATE:
                        raise UpdateClient()

                    else:
                        raise InvalidAction(f"cannot complete action={action}")

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
            LOGGER.error("unknown error")
            LOGGER.exception(e)
            raise ErrorClient()

        if self.stop:
            raise ErrorClient()

        return 0
