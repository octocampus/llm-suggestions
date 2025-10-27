"""
Pydantic models for LLM suggestions
"""

from pydantic import BaseModel, Field
from typing import List, Optional
from enum import Enum


class Severity(str, Enum):
    """Rule severity levels"""

    CRITICAL = "critical"
    HIGH = "high"
    MEDIUM = "medium"


class DataQualityRule(BaseModel):
    """A data quality issue found in a column"""

    column: str = Field(..., description="Column name with issues")
    issues: List[str] = Field(..., description="List of specific problems found")
    recommendation: str = Field(..., description="What to do to fix the issues")
    severity: Severity = Field(
        ..., description="Issue severity: critical, high, or medium"
    )


class SuggestionRequest(BaseModel):
    """Request to generate DQ suggestions"""

    source_key: str
    schema_name: str
    table_name: str
    limit: int = Field(default=100, ge=1, le=1000)


class SuggestionResponse(BaseModel):
    """Response with generated DQ suggestions"""

    source_key: str
    schema_name: str
    table_name: str
    rules: List[DataQualityRule]
    row_count_analyzed: int
    model_used: str
    metadata: Optional[dict] = None
