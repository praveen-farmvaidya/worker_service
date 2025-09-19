import os
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from sqlalchemy import text, select
from uuid import UUID
import structlog
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, String, DateTime, JSON, UUID as UUIDType

log = structlog.get_logger()
DATABASE_URL = os.getenv("CENTRAL_DATABASE_URL")
engine = create_async_engine(DATABASE_URL)
AsyncSessionLocal = sessionmaker(bind=engine, class_=AsyncSession)
Base = declarative_base()

# A lightweight reflection of the Job model for type hinting and querying
class Job(Base):
    __tablename__ = 'jobs'
    id = Column(UUIDType(as_uuid=True), primary_key=True)
    user_id = Column(UUIDType(as_uuid=True), index=True, nullable=False)
    model_identifier = Column(String)
    workflow_type = Column(String)
    blob_name = Column(String)
    input_data = Column(JSON)
    __table_args__ = {'extend_existing': True}


async def update_job_status_from_upload(job_id: UUID, new_status: str) -> bool:
    async with AsyncSessionLocal() as session:
        async with session.begin():
            try:
                stmt = text(f"UPDATE jobs SET status = :status WHERE id = :job_id")
                await session.execute(stmt, {'status': new_status, 'job_id': job_id})
                await session.commit()
                log.info("Updated job status to PENDING after upload.", job_id=str(job_id))
                return True
            except Exception as e:
                log.error("Failed to update job status from worker.", error=e, job_id=str(job_id))
                await session.rollback()
                return False

async def get_job_details(job_id: UUID) -> Job | None:
    async with AsyncSessionLocal() as session:
        try:
            result = await session.execute(select(Job).filter(Job.id == job_id))
            job = result.scalars().first()
            return job
        except Exception as e:
            log.error("Failed to fetch job details from worker DB util.", error=e, job_id=str(job_id))
            return None