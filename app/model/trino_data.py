from pydantic import BaseModel, Field
from typing import List, Dict, Any, Optional
from datetime import datetime


class ColumnMetadata(BaseModel):
    """Metadata for a single column"""

    column_name: str
    column_type: str


class TableSampleResponse(BaseModel):
    """Response model for table sample data"""

    source_key: str
    schema_name: str
    table_name: str
    columns: List[str]
    row_count: int
    rows: List[Dict[str, Any]]


class TableInfo(BaseModel):
    """Table information from discovery data"""

    source_id: str
    schema_name: str
    table_name: str
    columns: List[ColumnMetadata]


class TableRowCountResponse(BaseModel):
    """Response model for table row count"""

    source_key: str
    schema_name: str
    table_name: str
    total_rows: int
