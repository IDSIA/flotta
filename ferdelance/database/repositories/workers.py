from ferdelance.database.data import TYPE_WORKER
from ferdelance.database.repositories import Repository, AsyncSession
from ferdelance.database.tables import Token, Component

from sqlalchemy import select


class WorkerRepository(Repository):
    def __init__(self, session: AsyncSession) -> None:
        super().__init__(session)

    async def get_worker_token(self) -> str:
        """Get the token relative to the current active worker (there should be only one,
        and all the workers share the same token).

        Returns:
            str: the token of the current active worker.
        """
        res = await self.session.scalars(
            select(Token.token)
            .select_from(Token)
            .join(Component, Component.component_id == Token.component_id)
            .where(Component.type_name == TYPE_WORKER)
            .limit(1)
        )
        return res.one()
