"""
Trino connection utility
Place this in: utils/trino/connection_to_trino.py
"""
from trino.dbapi import connect
from app.core.config import settings
from app.core.logging import logger


def create_trino_cursor():
    """
    Create and return a Trino cursor
    
    Returns:
        Trino cursor object
    """
    try:
        # You'll need to add these to your config.py
        conn = connect(
            host=getattr(settings, 'trino_host', 'localhost'),
            port=getattr(settings, 'trino_port', 8080),
            user=getattr(settings, 'trino_user', 'trino'),
            catalog=getattr(settings, 'trino_catalog', 'hive'),
            schema=getattr(settings, 'trino_schema', 'default'),
            http_scheme='http',
            auth=None  # Add authentication if needed
        )
        
        cursor = conn.cursor()
        logger.info("Trino cursor created successfully")
        return cursor
        
    except Exception as e:
        logger.error(f"Failed to create Trino cursor: {str(e)}")
        raise Exception(f"Trino connection failed: {str(e)}")