from pydantic import BaseModel, Field
from datetime import datetime
from typing import Optional, List


# ========== External API Response Models ==========


class ColumnProfile(BaseModel):
    """Individual column profiling statistics from external API"""

    columnName: str
    dataType: str
    nullPercentage: float
    distinctCount: int
    minValue: Optional[float] = None
    maxValue: Optional[float] = None


class ProfilingDataResponse(BaseModel):
    """Complete profiling response from external API"""

    id: str
    sourceKey: str
    schemaName: str
    tableName: str
    columnCount: int
    rowCount: int
    timestamp: datetime
    columns: List[ColumnProfile]


# ========== Database Response Models ==========


class ColumnProfileResponse(BaseModel):
    """Column profile stored in our database"""

    id: int
    profiling_run_id: int
    column_name: str
    data_type: str
    null_percentage: float
    distinct_count: int
    min_value: Optional[float] = None
    max_value: Optional[float] = None

    model_config = {"from_attributes": True}


class ProfilingRunResponse(BaseModel):
    """Profiling run metadata stored in our database"""

    id: int
    profile_id: str
    source_key: str
    schema_name: str
    table_name: str
    column_count: int
    row_count: int
    profiled_at: datetime
    created_at: datetime
    columns: List[ColumnProfileResponse] = Field(default_factory=list)

    model_config = {"from_attributes": True}
