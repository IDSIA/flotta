from ferdelance.config import conf
from ferdelance.server.api import api

import uvicorn


if __name__ == "__main__":
    uvicorn.run(api, host=conf.SERVER_INTERFACE, port=conf.SERVER_PORT)
