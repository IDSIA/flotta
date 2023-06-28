from ferdelance.database.data import TYPE_WORKER
from ferdelance.database.repositories import Repository, AsyncSession
from ferdelance.database.tables import Token, Component

from sqlalchemy import select


class WorkerRepository(Repository):
    """Repository used to manage the components identified as workers.

    Currently, this repository is used only for testing purposes.
    """

    def __init__(self, session: AsyncSession) -> None:
        super().__init__(session)

    async def get_worker_token(self) -> tuple[str, str]:
        """Get the token relative to the current active worker (there should be
        only one record since all workers share the same token).

        Returns:
            str: the token of the current active worker.
        """
        res = await self.session.scalars(
            select(Token)
            .select_from(Token)
            .join(Component, Component.id == Token.component_id)
            .where(Component.type_name == TYPE_WORKER)
            .limit(1)
        )
        token = res.one()
        return token.token, token.component_id
