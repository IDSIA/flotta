from ferdelance.database.services.core import DBSessionService, AsyncSession
from ferdelance.database.tables import Application

from sqlalchemy import select


class ApplicationService(DBSessionService):
    def __init__(self, session: AsyncSession) -> None:
        super().__init__(session)

    async def get_newest_app(self) -> Application | None:
        result = await self.session.execute(
            select(Application)
            .where(Application.active)
            .order_by(Application.creation_time.desc())
            .limit(1)
        )
        return result.scalar_one_or_none()
