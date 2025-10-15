import logging
import sys
from app.core.config import settings


def setup_logging():
    """Configure application logging"""
    logging.basicConfig(
        level=settings.log_level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[
            logging.StreamHandler(sys.stdout)
        ]
    )
    return logging.getLogger(__name__)


logger = setup_logging()