from ferdelance.config import conf
from ferdelance.database import AsyncSession
from ferdelance.jobs.server import JobManagementService
from ferdelance.jobs.local import JobManagementLocalService


def job_manager(session: AsyncSession) -> JobManagementService:
    if conf.STANDALONE:
        return JobManagementLocalService(session)
    return JobManagementService(session)
