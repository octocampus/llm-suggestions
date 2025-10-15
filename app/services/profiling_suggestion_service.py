from sqlalchemy.orm import Session
from typing import List, Optional
import httpx
from app.db.schema import ProfilingRun, ColumnProfile
from app.model.profiling_suggestion import (
    ProfilingDataResponse,
    ProfilingRunResponse,
    ColumnProfileResponse,
)
from app.core.config import settings
from app.core.logging import logger


class ProfilingSuggestionService:
    """Service layer for profiling data management"""

    def __init__(self, db: Session):
        self.db = db

    async def fetch_profiling_data(
        self, source_key: str, schema_name: str, table_name: str
    ) -> ProfilingDataResponse:
        """Fetch JSON profiling data from external API"""
        params = {
            "sourceKey": source_key,
            "schemaName": schema_name,
            "tableName": table_name,
        }

        logger.info(f"Fetching: {schema_name}.{table_name}")

        async with httpx.AsyncClient(timeout=settings.external_api_timeout) as client:
            response = await client.get(settings.external_api_base_url, params=params)
            response.raise_for_status()
            data = response.json()
            logger.info(f"Fetched profile ID: {data.get('id')}")
            return ProfilingDataResponse(**data)

    def save_profiling_data(
        self, profiling_data: ProfilingDataResponse
    ) -> ProfilingRunResponse:
        """Save JSON data to database"""
        # Check if already exists
        existing = (
            self.db.query(ProfilingRun)
            .filter(ProfilingRun.profile_id == profiling_data.id)
            .first()
        )

        if existing:
            logger.info(f"Already exists: {profiling_data.id}")
            return self._build_response(existing)

        # Save profiling run
        profiling_run = ProfilingRun(
            profile_id=profiling_data.id,
            source_key=profiling_data.sourceKey,
            schema_name=profiling_data.schemaName,
            table_name=profiling_data.tableName,
            column_count=profiling_data.columnCount,
            row_count=profiling_data.rowCount,
            profiled_at=profiling_data.timestamp,
        )
        self.db.add(profiling_run)
        self.db.flush()

        # Save columns
        for col in profiling_data.columns:
            self.db.add(
                ColumnProfile(
                    profiling_run_id=profiling_run.id,
                    column_name=col.columnName,
                    data_type=col.dataType,
                    null_percentage=col.nullPercentage,
                    distinct_count=col.distinctCount,
                    min_value=col.minValue,
                    max_value=col.maxValue,
                )
            )

        self.db.commit()
        self.db.refresh(profiling_run)
        logger.info(f"Saved: {profiling_run.schema_name}.{profiling_run.table_name}")
        return self._build_response(profiling_run)

    async def fetch_and_save(
        self, source_key: str, schema_name: str, table_name: str
    ) -> ProfilingRunResponse:
        """Main workflow: fetch JSON from API and save to DB"""
        data = await self.fetch_profiling_data(source_key, schema_name, table_name)
        return self.save_profiling_data(data)

    def get_profiling_runs(
        self,
        table_name: Optional[str] = None,
        source_key: Optional[str] = None,
        limit: int = 100,
    ) -> List[ProfilingRunResponse]:
        """Query stored profiling data"""
        query = self.db.query(ProfilingRun)
        if table_name:
            query = query.filter(ProfilingRun.table_name == table_name)
        if source_key:
            query = query.filter(ProfilingRun.source_key == source_key)
        runs = query.order_by(ProfilingRun.created_at.desc()).limit(limit).all()
        return [self._build_response(run) for run in runs]

    def get_profiling_run_by_id(
        self, profile_id: str
    ) -> Optional[ProfilingRunResponse]:
        """Get specific profile by ID"""
        run = (
            self.db.query(ProfilingRun)
            .filter(ProfilingRun.profile_id == profile_id)
            .first()
        )
        return self._build_response(run) if run else None

    def _build_response(self, profiling_run: ProfilingRun) -> ProfilingRunResponse:
        """Build response object"""
        return ProfilingRunResponse(
            id=profiling_run.id,
            profile_id=profiling_run.profile_id,
            source_key=profiling_run.source_key,
            schema_name=profiling_run.schema_name,
            table_name=profiling_run.table_name,
            column_count=profiling_run.column_count,
            row_count=profiling_run.row_count,
            profiled_at=profiling_run.profiled_at,
            created_at=profiling_run.created_at,
            columns=[
                ColumnProfileResponse(
                    id=col.id,
                    profiling_run_id=col.profiling_run_id,
                    column_name=col.column_name,
                    data_type=col.data_type,
                    null_percentage=col.null_percentage,
                    distinct_count=col.distinct_count,
                    min_value=col.min_value,
                    max_value=col.max_value,
                )
                for col in profiling_run.columns
            ],
        )


def get_profiling_suggestion_service(db: Session):
    return ProfilingSuggestionService(db)
