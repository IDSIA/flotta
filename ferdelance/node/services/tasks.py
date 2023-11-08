from ferdelance.database.repositories import AsyncSession, Repository
from ferdelance.schemas.components import Component
from ferdelance.schemas.tasks import Task


class TaskManagementService(Repository):
    def __init__(self, session: AsyncSession, component: Component) -> None:
        super().__init__(session)

        self.component: Component = component

    async def task_start(self, task: Task) -> None:
        # TODO: start locally
        ...
