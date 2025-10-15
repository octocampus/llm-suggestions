from sqlalchemy import (
    Column,
    Integer,
    String,
    DateTime,
    Float,
    ForeignKey,
    create_engine,
    JSON,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship
from datetime import datetime
from app.core.config import settings

Base = declarative_base()


class ProfilingRun(Base):
    """Stores metadata about a profiling run"""

    __tablename__ = "profiling_runs"

    id = Column(Integer, primary_key=True, index=True)
    profile_id = Column(String, unique=True, nullable=False, index=True)
    source_key = Column(String, nullable=False, index=True)
    schema_name = Column(String, nullable=False)
    table_name = Column(String, nullable=False, index=True)
    column_count = Column(Integer, nullable=False)
    row_count = Column(Integer, nullable=False)
    profiled_at = Column(DateTime, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)

    # Relationship to column profiles
    columns = relationship(
        "ColumnProfile", back_populates="profiling_run", cascade="all, delete-orphan"
    )


class ColumnProfile(Base):
    """Stores profiling data for individual columns"""

    __tablename__ = "column_profiles"

    id = Column(Integer, primary_key=True, index=True)
    profiling_run_id = Column(Integer, ForeignKey("profiling_runs.id"), nullable=False)
    column_name = Column(String, nullable=False, index=True)
    data_type = Column(String, nullable=False)
    null_percentage = Column(Float, nullable=False)
    distinct_count = Column(Integer, nullable=False)
    min_value = Column(Float, nullable=True)
    max_value = Column(Float, nullable=True)

    # Relationship to profiling run
    profiling_run = relationship("ProfilingRun", back_populates="columns")


class DiscoveryData(Base):
    """Stores discovery data from PostgreSQL"""

    __tablename__ = "discovery_data"

    id = Column(String, primary_key=True)
    schemas = Column(JSON)
    timestamp = Column(DateTime(timezone=True))
    source_id = Column(String(255))


# Database setup
engine = create_engine(
    settings.database_url,
    connect_args=(
        {"check_same_thread": False} if "sqlite" in settings.database_url else {}
    ),
)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


def get_db():
    """Database session dependency"""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def init_db():
    """Initialize database tables"""
    Base.metadata.create_all(bind=engine)
