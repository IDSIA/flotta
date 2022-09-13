from typing import Any
import requests
import logging

from ferdelance_workbench.exceptions import ServerError

LOGGER = logging.getLogger(__name__)


class Context:

    def __init__(self, server: str) -> None:
        self.server = server.rstrip('/')

    def list_clients(self) -> list[str]:
        res = requests.get(f'{self.server}/workbench/client/list')

        if res.status_code != 200:
            raise ServerError(f'server status code: {res.status_code}')

        return res.json()

    def detail_client(self, client_id: str) -> dict[str, Any]:
        res = requests.get(f'{self.server}/workbench/client/{client_id}')

        if res.status_code != 200:
            raise ServerError(f'server status code: {res.status_code}')

        return res.json()

    def list_datasources(self) -> list[dict]:
        res = requests.get(f'{self.server}/workbench/datasource/list/')

        if res.status_code != 200:
            raise ServerError(f'server status code: {res.status_code}')

        return res.json()

    def detail_datasource(self, datasource_id: int):
        res = requests.get(f'{self.server}/workbench/datasource/{datasource_id}')

        if res.status_code != 200:
            raise ServerError(f'server status code: {res.status_code}')

        return res.json()
