from urllib.parse import quote_plus
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import QueuePool
from contextlib import contextmanager

from app.core.config import settings, get_postgres_config
from app.core.logging import logger


def init_engine(database: str = None, pool_size=5, max_overflow=10):
    postgres_config = get_postgres_config()
    if database is not None:
        postgres_config["database"] = database

    password = quote_plus(postgres_config["password"])
    engine_url = (
        f"postgresql://{postgres_config['user']}:{password}"
        f"@{postgres_config['host']}:{postgres_config['port']}/{postgres_config['database']}"
    )

    logger.info(f"Connecting to PostgreSQL: {postgres_config['host']}:{postgres_config['port']}/{postgres_config['database']}")

    engine = create_engine(
        engine_url,
        poolclass=QueuePool,
        pool_size=pool_size,
        max_overflow=max_overflow,
        pool_timeout=30,
        pool_recycle=1800,
        pool_pre_ping=True,
        echo=settings.debug,  # Log SQL queries in debug mode
        future=True,
    )
    
    # Test connection on initialization
    try:
        with engine.connect() as conn:
            result = conn.execute(text("SELECT version()"))
            version = result.fetchone()[0]
            logger.info(f"✓ PostgreSQL connection successful")
            logger.info(f"PostgreSQL version: {version[:100]}")
            
            # Check if discovery_data table exists
            result = conn.execute(text("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                    AND table_name = 'discovery_data'
                )
            """))
            exists = result.fetchone()[0]
            
            if exists:
                result = conn.execute(text("SELECT COUNT(*) FROM discovery_data"))
                count = result.fetchone()[0]
                logger.info(f"✓ Table 'discovery_data' found with {count} records")
            else:
                logger.warning("✗ Table 'discovery_data' does not exist!")
                
    except Exception as e:
        logger.error(f"✗ PostgreSQL connection failed: {e}")
        raise
    
    return engine


engine = init_engine()

SessionLocal = sessionmaker(bind=engine, expire_on_commit=False, autoflush=False)


@contextmanager
def get_postgres_session():
    session = SessionLocal()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def get_db():
    """Database session generator for FastAPI dependency injection"""
    session = SessionLocal()
    try:
        yield session
    finally:
        session.close()