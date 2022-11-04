from .core import DBSessionService, AsyncSession

from ..tables import ClientApp

from sqlalchemy import select


class ClientAppService(DBSessionService):

    def __init__(self, session: AsyncSession) -> None:
        super().__init__(session)

    async def get_newest_app(self) -> ClientApp | None:
        result = await self.session.execute(
            select(ClientApp)
            .where(ClientApp.active)
            .order_by(ClientApp.creation_time.desc())
            .limit(1)
        )
        return result.scalar_one_or_none()
