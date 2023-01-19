from ferdelance.config import conf
from ferdelance.server.api import api

from multiprocessing import Process

import uvicorn


class LocalServer(Process):
    def run(self) -> None:
        uvicorn.run(api, host=conf.SERVER_INTERFACE, port=conf.SERVER_PORT)
