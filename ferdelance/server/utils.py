from ferdelance.config import conf
from ferdelance.database import AsyncSession
from ferdelance.server.services import JobManagementService
from ferdelance.standalone.services import JobManagementLocalService


def job_manager(session: AsyncSession) -> JobManagementService:
    if conf.STANDALONE:
        return JobManagementLocalService(session)
    return JobManagementService(session)
