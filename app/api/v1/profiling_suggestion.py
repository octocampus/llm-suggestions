from fastapi import APIRouter, HTTPException, Query
from typing import List, Optional

from app.db.schema import get_db
from app.services.profiling_suggestion_service import ProfilingSuggestionService
from app.services.postgres_service import get_postgres_service
from app.services.trino_source_service import create_trino_data_fetch_service
from app.model.profiling_suggestion import ProfilingRunResponse
# Assuming you have this import
from app.db.connection_to_trino import create_trino_cursor


router = APIRouter(prefix="/api/v1/profiling", tags=["profiling"])


@router.post("/fetch", status_code=201)
async def fetch_and_store_profile(
    source_key: str = Query(..., description="Source key (e.g., nemo_telecom_data)"),
    schema_name: str = Query(
        ..., description="Schema name (e.g., billing_finance_space)"
    ),
    table_name: str = Query(..., description="Table name (e.g., billing_transactions)"),
):
    try:
        # Create service instance directly
        db = next(get_db())
        service = ProfilingSuggestionService(db)
        return await service.fetch_and_save(source_key, schema_name, table_name)
    except ValueError as e:
        raise HTTPException(status_code=502, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal error: {str(e)}")


# Optional GET alias for clients that use GET instead of POST
@router.get("/fetch")
async def fetch_and_store_profile_get(
    source_key: str = Query(..., description="Source key (e.g., nemo_telecom_data)"),
    schema_name: str = Query(
        ..., description="Schema name (e.g., billing_finance_space)"
    ),
    table_name: str = Query(..., description="Table name (e.g., billing_transactions)"),
):
    db = next(get_db())
    service = ProfilingSuggestionService(db)
    return await service.fetch_and_save(source_key, schema_name, table_name)


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


# NEW ENDPOINTS FOR FETCHING RAW DATA FROM TRINO

@router.get("/trino/table/sample")
def get_table_sample_from_trino(
    source_key: str = Query(..., description="Trino catalog/source key (e.g., nemo_telecom_data)"),
    schema_name: str = Query(..., description="Schema name (e.g., billing_finance_space)"),
    table_name: str = Query(..., description="Table name (e.g., billing_transactions)"),
    limit: int = Query(100, ge=1, le=1000, description="Number of rows to fetch"),
):

    cursor = None
    try:
        cursor = create_trino_cursor()
        service = create_trino_data_fetch_service(cursor)
        return service.get_table_sample_data(source_key, schema_name, table_name, limit)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching table sample: {str(e)}")
    finally:
        if cursor:
            cursor.close()


@router.get("/trino/tables/from-discovery")
def get_tables_from_discovery(
    source_id: str = Query(..., description="Source ID from discovery data"),
    schema_filter: Optional[str] = Query(None, description="Optional schema name to filter"),
):

    cursor = None
    try:
        cursor = create_trino_cursor()
        service = create_trino_data_fetch_service(cursor)
        return service.get_tables_from_discovery(source_id, schema_filter)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error getting tables from discovery: {str(e)}")
    finally:
        if cursor:
            cursor.close()


@router.get("/trino/tables/sample-all")
def get_all_tables_sample_from_trino(
    source_key: str = Query(..., description="Trino catalog/source key"),
    source_id: str = Query(..., description="Source ID for discovery data"),
    schema_filter: Optional[str] = Query(None, description="Optional schema name to filter"),
    limit_per_table: int = Query(100, ge=1, le=1000, description="Rows to fetch per table"),
):

    cursor = None
    try:
        cursor = create_trino_cursor()
        service = create_trino_data_fetch_service(cursor)
        return service.fetch_all_tables_sample_data(
            source_key, source_id, schema_filter, limit_per_table
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching all tables sample: {str(e)}")
    finally:
        if cursor:
            cursor.close()


@router.get("/trino/table/count")
def get_table_row_count(
    source_key: str = Query(..., description="Trino catalog/source key"),
    schema_name: str = Query(..., description="Schema name"),
    table_name: str = Query(..., description="Table name"),
):

    cursor = None
    try:
        cursor = create_trino_cursor()
        service = create_trino_data_fetch_service(cursor)
        count = service.get_table_row_count(source_key, schema_name, table_name)
        return {
            "source_key": source_key,
            "schema_name": schema_name,
            "table_name": table_name,
            "total_rows": count
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error getting row count: {str(e)}")
    finally:
        if cursor:
            cursor.close()


@router.get("/")
def get_profiling_runs(
    table_name: Optional[str] = Query(None, description="Filter by table name"),
    source_key: Optional[str] = Query(None, description="Filter by source key"),
    limit: int = Query(100, ge=1, le=1000),
):

    db = next(get_db())
    service = ProfilingSuggestionService(db)
    return service.get_profiling_runs(table_name, source_key, limit)


@router.get("/{profile_id}")
def get_profiling_run(profile_id: str):
 
    db = next(get_db())
    service = ProfilingSuggestionService(db)
    result = service.get_profiling_run_by_id(profile_id)
    if not result:
        raise HTTPException(status_code=404, detail="Profiling run not found")
    return result