from sqlalchemy.ext.asyncio import AsyncSession


class DBSessionService:
    def __init__(self, session: AsyncSession) -> None:
        self.session: AsyncSession = session
