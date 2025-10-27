from sqlalchemy import Column, DateTime, String
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class DiscoveryData(Base):
    """Stores discovery data from PostgreSQL"""

    __tablename__ = "discovery_data"

    id = Column(String, primary_key=True)
    schemas = Column(JSONB)  # Use JSONB for PostgreSQL
    timestamp = Column(DateTime(timezone=True))
    source_id = Column(String(255))

    def __repr__(self):
        return f"<DiscoveryData(id={self.id}, source_id={self.source_id}, timestamp={self.timestamp})>"
