from typing import Any, Dict, List, Optional
from sqlalchemy import func, cast, String, text
from sqlalchemy.exc import SQLAlchemyError
from app.core.config import settings
from app.core.logging import logger
from app.model.discovery import DiscoveryData
from app.db.postgres import get_db  # Changed from app.db.schema to app.core.postgres


class PostgresService:
    def __init__(self):
        pass

    def query_discovery_data(
        self, schema: str, source_id: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """Query discovery_data table using SQLAlchemy ORM"""
        db = None
        try:
            db = next(get_db())

            query = db.query(DiscoveryData)

            query = query.filter(
                func.cast(DiscoveryData.schemas, String).like(f'%"schema_name": "{schema}"%')
            )


            if source_id:
                query = query.filter(DiscoveryData.source_id == source_id)

            query = query.order_by(DiscoveryData.timestamp.desc())

            logger.info(
                f"Query parameters - schema: '{schema}', source_id: '{source_id}'"
            )

            total_count = db.query(DiscoveryData).count()
            logger.info(f"Total records in discovery_data: {total_count}")


            if source_id:
                source_id_count = (
                    db.query(DiscoveryData)
                    .filter(DiscoveryData.source_id == source_id)
                    .count()
                )
                logger.info(f"Records with source_id '{source_id}': {source_id_count}")
                

                available_ids = db.query(DiscoveryData.source_id).distinct().limit(10).all()
                logger.info(f"Sample available source_ids: {[s[0] for s in available_ids]}")

            schema_count = (
                db.query(DiscoveryData)
                .filter(DiscoveryData.schemas.contains([schema]))
                .count()
            )
            logger.info(f"Records with schema '{schema}': {schema_count}")


            results = query.all()


            data = []
            for row in results:
                data.append(
                    {
                        "id": row.id,
                        "schemas": row.schemas,
                        "timestamp": (
                            row.timestamp.isoformat() if row.timestamp else None
                        ),
                        "source_id": row.source_id,
                    }
                )

            logger.info(f"Retrieved {len(data)} discovery records")
            return data

        except Exception as e:
            logger.error(f"PostgreSQL query failed: {e}", exc_info=True)
            raise Exception(f"PostgreSQL query failed: {e}")
        finally:
            if db is not None:
                db.close()

    @staticmethod
    def get_latest_schemas_from_db(session, source_id: Optional[str] = None):
        """Get latest schemas for each source_id using subquery"""
        try:
            # Subquery to get max timestamp for each source_id
            subquery = (
                session.query(
                    DiscoveryData.source_id,
                    func.max(DiscoveryData.timestamp).label("max_timestamp"),
                )
                .group_by(DiscoveryData.source_id)
                .subquery()
            )

            # Main query to get latest schemas
            query = session.query(
                DiscoveryData.schemas,
                DiscoveryData.timestamp,
                DiscoveryData.source_id,
            ).join(
                subquery,
                (DiscoveryData.source_id == subquery.c.source_id)
                & (DiscoveryData.timestamp == subquery.c.max_timestamp),
            )

            # Filter by source_id if provided
            if source_id:
                query = query.filter(DiscoveryData.source_id == source_id)

            results = query.all()
            return results

        except Exception as e:
            logger.error(f"Error getting latest schemas: {e}", exc_info=True)
            raise Exception(f"Error getting latest schemas: {e}")

    @staticmethod
    def save_to_db(data, session=None):
        """Save data to database using SQLAlchemy ORM"""
        if session is not None:
            try:
                if isinstance(data, list):
                    session.bulk_save_objects(data)
                else:
                    session.add(data)
                session.flush()
                return True
            except SQLAlchemyError as e:
                session.rollback()
                logger.error(f"Error saving to database: {e}", exc_info=True)
                return False

        db = None
        try:
            db = next(get_db())
            if isinstance(data, list):
                db.bulk_save_objects(data)
            else:
                db.add(data)
            db.commit()
            return True
        except SQLAlchemyError as e:
            if db:
                db.rollback()
            logger.error(f"Error saving to database: {e}", exc_info=True)
            return False
        finally:
            if db is not None:
                db.close()


def get_postgres_service():
    """Factory function for PostgresService"""
    return PostgresService()