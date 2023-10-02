from ferdelance.config import config_manager
from ferdelance.schemas.database import Resource
from ferdelance.database.tables import Resource as ResourceDB
from ferdelance.database.repositories.core import AsyncSession, Repository

from sqlalchemy import select
from uuid import uuid4


def view(resource: ResourceDB) -> Resource:
    return Resource(
        id=resource.id,
        creation_time=resource.creation_time,
        path=resource.path,
        component_id=resource.component_id,
    )


class ResourceRepository(Repository):
    def __init__(self, session: AsyncSession) -> None:
        super().__init__(session)

    async def create_resource(self, component_id: str) -> Resource:
        resource_id = str(uuid4())

        out_path = config_manager.get().storage_resources(f"{resource_id}.bin")

        resource = ResourceDB(
            id=resource_id,
            path=out_path,
            component_id=component_id,
        )

        self.session.add(resource)
        await self.session.commit()
        await self.session.refresh(resource)

        return view(resource)

    async def get_resource(self, resource_id: str) -> Resource:
        res = await self.session.scalars(
            select(ResourceDB).where(
                ResourceDB.id == resource_id,
            )
        )
        return view(res.one())
