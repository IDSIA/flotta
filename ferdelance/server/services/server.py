from ferdelance.database import AsyncSession

import logging

LOGGER = logging.getLogger(__name__)


class ServerService:
    def __init__(self, session: AsyncSession, component_id: str) -> None:
        self.session: AsyncSession = session
        self.component_id: str = component_id
