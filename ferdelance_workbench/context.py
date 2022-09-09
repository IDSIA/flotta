import requests
import logging

LOGGER = logging.getLogger(__name__)


class Context:

    def __init__(self, server: str) -> None:
        self.server = server

    def list_clients(self) -> list[str]:
        res = requests.get(
            f'{self.server}/manager/client/list'
        )

        if res.status_code != 200:
            LOGGER.error(f'server answered with status code: {res.status_code}')
            return []

        return res.json()
