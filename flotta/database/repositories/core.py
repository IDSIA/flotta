from sqlalchemy.ext.asyncio import AsyncSession


class Repository:
    """Base class for all repositories. This class guarantees the presence of a
    database session object.
    """

    def __init__(self, session: AsyncSession) -> None:
        self.session: AsyncSession = session
