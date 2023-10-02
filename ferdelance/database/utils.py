from ferdelance.config import DatabaseConfiguration, config_manager

from sqlalchemy.engine import URL


def db_connection_url(sync: bool = False) -> URL | str:
    conf: DatabaseConfiguration = config_manager.get().database

    driver: str = ""

    if conf.memory:
        if not sync:
            driver = "+aiosqlite"

        return f"sqlite{driver}://"

    dialect = conf.dialect.lower()

    assert conf.host is not None

    if dialect == "sqlite":
        if not sync:
            driver = "+aiosqlite"

        # in this case host is an absolute path
        return f"sqlite{driver}:///{conf.host}"

    if dialect == "postgresql":
        assert conf.username is not None
        assert conf.password is not None
        assert conf.port is not None

        if not sync:
            driver = "+asyncpg"

        return URL.create(
            f"postgresql{driver}",
            conf.username,
            conf.password,
            conf.host,
            conf.port,
            conf.scheme,
        )

    raise ValueError(f"dialect {dialect} is not supported")
