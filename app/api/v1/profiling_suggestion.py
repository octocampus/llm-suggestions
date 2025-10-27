from fastapi import APIRouter, HTTPException, Query
from typing import List, Optional

from app.db.schema import get_db
from app.services.postgres_service import get_postgres_service
from app.services.trino_source_service import create_trino_data_fetch_service
from app.model.trino_data import (
    TableSampleResponse,
    TableInfo,
    TableRowCountResponse,
)
from app.model.llm_sugg_models import SuggestionResponse
from app.services.llm_sugg_service import LLMSuggestionsService
from app.core.config import settings
from app.core.logging import logger

# Assuming you have this import
from app.db.connection_to_trino import create_trino_cursor


router = APIRouter(prefix="/api/v1/profiling", tags=["profiling"])


@router.get("/discovery")
def get_discovery_data(
    schema: str = Query(..., description="Schema name to filter by"),
    source_id: Optional[str] = Query(None, description="Filter by source_id"),
):
    """Query discovery_data table from PostgreSQL using SQLAlchemy ORM"""
    try:
        postgres_service = get_postgres_service()
        return postgres_service.query_discovery_data(schema, source_id)
    except Exception as e:
        raise HTTPException(status_code=502, detail=str(e))





@router.get("/trino/table/sample", response_model=TableSampleResponse)
def get_table_sample_from_trino(
    source_key: str = Query(
        ..., description="Trino catalog/source key (e.g., nemo_telecom_data)"
    ),
    schema_name: str = Query(
        ..., description="Schema name (e.g., billing_finance_space)"
    ),
    table_name: str = Query(..., description="Table name (e.g., billing_transactions)"),
    limit: int = Query(100, ge=1, le=1000, description="Number of rows to fetch"),
) -> TableSampleResponse:
    """Fetch sample data from a Trino table"""
    cursor = None
    try:
        cursor = create_trino_cursor()
        service = create_trino_data_fetch_service(cursor)
        return service.get_table_sample_data(source_key, schema_name, table_name, limit)
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Error fetching table sample: {str(e)}"
        )
    finally:
        if cursor:
            cursor.close()


@router.get("/trino/tables/from-discovery", response_model=List[TableInfo])
def get_tables_from_discovery(
    source_id: str = Query(..., description="Source ID from discovery data"),
    schema_filter: Optional[str] = Query(
        None, description="Optional schema name to filter"
    ),
    table_filter: Optional[str] = Query(
        None, description="Optional table name to filter"
    ),
) -> List[TableInfo]:
    """Get table metadata from discovery data with optional schema/table filters"""
    cursor = None
    try:
        cursor = create_trino_cursor()
        service = create_trino_data_fetch_service(cursor)
        return service.get_tables_from_discovery(source_id, schema_filter, table_filter)
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Error getting tables from discovery: {str(e)}"
        )
    finally:
        if cursor:
            cursor.close()


@router.get("/trino/tables/sample-all", response_model=List[TableSampleResponse])
def get_all_tables_sample_from_trino(
    source_key: str = Query(..., description="Trino catalog/source key"),
    source_id: str = Query(..., description="Source ID for discovery data"),
    schema_filter: Optional[str] = Query(
        None, description="Optional schema name to filter"
    ),
    table_filter: Optional[str] = Query(
        None, description="Optional table name to filter"
    ),
    limit_per_table: int = Query(
        100, ge=1, le=1000, description="Rows to fetch per table"
    ),
) -> List[TableSampleResponse]:
    """Fetch sample data for multiple tables matching filters"""
    cursor = None
    try:
        cursor = create_trino_cursor()
        service = create_trino_data_fetch_service(cursor)
        return service.fetch_all_tables_sample_data(
            source_key, source_id, schema_filter, table_filter, limit_per_table
        )
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Error fetching all tables sample: {str(e)}"
        )
    finally:
        if cursor:
            cursor.close()


@router.get("/trino/table/count", response_model=TableRowCountResponse)
def get_table_row_count(
    source_key: str = Query(..., description="Trino catalog/source key"),
    schema_name: str = Query(..., description="Schema name"),
    table_name: str = Query(..., description="Table name"),
) -> TableRowCountResponse:
    """Get total row count for a table"""
    cursor = None
    try:
        cursor = create_trino_cursor()
        service = create_trino_data_fetch_service(cursor)
        return service.get_table_row_count(source_key, schema_name, table_name)
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Error getting row count: {str(e)}"
        )
    finally:
        if cursor:
            cursor.close()


@router.post("/llm/suggestions", response_model=SuggestionResponse)
def generate_llm_suggestions(
    source_key: str = Query(..., description="Trino catalog/source key"),
    schema_name: str = Query(..., description="Schema name"),
    table_name: str = Query(..., description="Table name"),
    limit: int = Query(100, ge=1, le=1000, description="Sample size for analysis"),
    provider: str = Query(
        None, description="LLM provider (optional, uses config default)"
    ),
    model: str = Query(None, description="Model name (optional, uses config default)"),
) -> SuggestionResponse:
    """Generate data quality rule suggestions using LLM"""
    cursor = None
    try:
        
        cursor = create_trino_cursor()
        trino_service = create_trino_data_fetch_service(cursor)
        sample_data = trino_service.get_table_sample_data(
            source_key, schema_name, table_name, limit
        )

       
        postgres_service = get_postgres_service()
        source_id = getattr(
            settings, "default_source_id", None
        )  # You might need to get this from somewhere

        # Extract columns with types (combine from sample and discovery if needed)
        columns = []
        for col_name in sample_data.columns:
            columns.append({"column_name": col_name, "column_type": "unknown"})

        # Generate suggestions using LLM
        llm_service = LLMSuggestionsService()
        response = llm_service.generate_suggestions_response(
            source_key=source_key,
            schema_name=schema_name,
            table_name=table_name,
            columns=columns,
            sample_rows=sample_data.rows,
        )

        return response

    except Exception as e:
        logger.error(f"Failed to generate LLM suggestions: {e}")
        raise HTTPException(
            status_code=500, detail=f"Failed to generate suggestions: {str(e)}"
        )
    finally:
        if cursor:
            cursor.close()
