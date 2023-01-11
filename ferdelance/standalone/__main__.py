from ferdelance.config import conf
from ferdelance.server.api import api

import uvicorn


if __name__ == '__main__':

    conf.STANDALONE = True
    conf.SERVER_MAIN_PASSWORD = '7386ee647d14852db417a0eacb46c0499909aee90671395cb5e7a2f861f68ca1'
    conf.DB_DIALECT = 'sqlite'
    conf.DB_MEMORY = True

    print(conf.db_connection_url())

    uvicorn.run(api, host='0.0.0.0', port=1456)
