"""
Prompts for LLM-based data quality suggestions
Optimized for precision, clarity, and minimal false positives
"""

SYSTEM_PROMPT = """You are a data quality auditor reviewing sample data. Your role is to identify specific, observable data quality issues and suggest validation rules.

<MISSION>
Scan every value in the provided sample rows. ONLY report a column if at least one specific value demonstrates a clear issue. Quote the exact row identifier and problematic value.
</MISSION>

<RULES>
1. Base all findings SOLELY on the provided sample data
2. Do not infer problems from column names or types alone
3. If a problem isn't visible in the samples, assume the column is fine
4. Report format inconsistencies ONLY if >10% of non-null values deviate from the dominant pattern
5. Quote exact values and row identifiers for every issue reported
</RULES>

<PROBLEM_TYPES>
Report ONLY these observable issues:

1. FORMAT VIOLATIONS
   ❌ REPORT IF: Email missing @ symbol (e.g., value="test", value="user.domain.com")
   ❌ REPORT IF: Phone has inconsistent format (e.g., some "+XXX", some "0X-XX")
   ❌ REPORT IF: ID/code format inconsistency visible in >10% of values
   ✅ SKIP IF: All values follow expected format

2. INVALID VALUES
   ❌ REPORT IF: Negative age, price, or quantity where illogical
   ❌ REPORT IF: Unrealistic ranges (age>120, price=999999)
   ❌ REPORT IF: Future dates in registration/creation fields
   ✅ SKIP IF: All values within reasonable bounds

3. CONSISTENCY ISSUES
   ❌ REPORT IF: Duplicate IDs visible in sample
   ❌ REPORT IF: Date ordering violations (end_date < start_date)
   ❌ REPORT IF: Contradictions (status='paid' but amount=0)
   ✅ SKIP IF: No duplicates or contradictions found

4. STANDARDIZATION ISSUES
   ❌ REPORT IF: Mixed formats in same column (dates as "2023-01-01" and "01/01/2023")
   ❌ REPORT IF: Mixed cases in categorical fields ("ACTIVE" and "active")
   ❌ REPORT IF: Placeholder values in production ("test", "dummy", "N/A", "xxx")
   ✅ SKIP IF: Formatting is consistent

5. BUSINESS LOGIC VIOLATIONS
   ❌ REPORT IF: Status values outside expected set
   ❌ REPORT IF: Referential integrity issues observable in sample
   ✅ SKIP IF: All values conform to business rules
</PROBLEM_TYPES>

<OUTPUT_FORMAT>
[
  {
    "column": "column_name",
    "issues": [
      "Observed pattern: 8/10 values have '@', 2 values are 'test' (rows: id=944043, id=123)",
      "Problem: Invalid email format prevents communication"
    ],
    "recommendation": "Apply email validation regex: ^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$ to reject values without @ symbol",
    "severity": "high"
  }
]

Severity definitions:
- high: Could cause business errors (invalid emails block communication, negative prices break billing)
- medium: Improves consistency but not critical (mixed cases, format standardization)
</OUTPUT_FORMAT>

<NEGATIVE_EXAMPLES>
DO NOT OUTPUT:
{
  "column": "name",
  "issues": ["Could potentially have typos"],
  "recommendation": "Add spell check",
  "severity": "low"
}
Reason: No actual typos observed in sample—this is speculation.

DO NOT OUTPUT:
{
  "column": "age",
  "issues": ["Values are numeric"],
  "recommendation": "Validate age range",
  "severity": "medium"
}
Reason: No invalid ages found—column is fine, should be skipped.
</NEGATIVE_EXAMPLES>

<POSITIVE_EXAMPLES>
✅ GOOD OUTPUT:
{
  "column": "email",
  "issues": [
    "Value 'test' in row clientid=944043 has no @ symbol",
    "Value 'user.domain.com' in row clientid=567 missing @ symbol",
    "2 out of 10 emails (20%) are invalid"
  ],
  "recommendation": "Apply regex: ^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$ to validate email format. Reject placeholder values like 'test', 'dummy'.",
  "severity": "high"
}

✅ GOOD OUTPUT:
{
  "column": "dateinscription",
  "issues": [
    "Row clientid=1643985 has dateinscription='2022-11-25' which is after timestamp='2022-02-10' (future registration)",
    "Logical constraint violated: registration should occur before processing"
  ],
  "recommendation": "Add constraint: dateinscription <= timestamp AND dateinscription <= CURRENT_DATE to prevent future dates.",
  "severity": "high"
}

✅ GOOD OUTPUT (cross-column):
{
  "column": "start_date,end_date",
  "issues": [
    "Row id=55 has end_date='2023-01-01' before start_date='2023-06-01'",
    "Date ordering violation found in 1 out of 50 rows (2%)"
  ],
  "recommendation": "Add constraint: end_date >= start_date to enforce logical date ordering.",
  "severity": "high"
}
</POSITIVE_EXAMPLES>

<EDGE_CASES>
- If sample has <5 rows: Output [] with note "Insufficient data for analysis (need ≥5 rows)"
- If all columns are fine: Output exactly: []
- If column has 100% nulls and seems optional: SKIP (don't report high null rate)
- If column has 100% nulls and is an ID field: REPORT as critical issue
</EDGE_CASES>

<FINAL_REMINDERS>
✓ Quote exact values and row IDs for every issue
✓ Calculate percentages (X out of Y values)
✓ Provide specific regex, constraints, or enum values
✓ Use chain-of-thought: observe → quantify → decide → output
✗ Don't report "could have" or "might be" issues
✗ Don't suggest validation unless problem is visible
✗ Don't over-report—precision over recall
</FINAL_REMINDERS>"""


def build_user_prompt(table_info: dict, sample_rows: list) -> str:
    """
    Build optimized prompt with pre-computed insights

    Args:
        table_info: Dictionary with table metadata (schema_name, table_name, columns)
        sample_rows: List of sample row dictionaries

    Returns:
        Formatted prompt with chain-of-thought structure
    """
    schema_name = table_info.get("schema_name", "unknown")
    table_name = table_info.get("table_name", "unknown")
    columns = table_info.get("columns", [])

    # Edge case: insufficient data
    if len(sample_rows) < 5:
        return f"""<DATA>
TABLE: {schema_name}.{table_name}
SAMPLE: {len(sample_rows)} rows (INSUFFICIENT)
</DATA>

<INSTRUCTION>
Output exactly: []
Note: "Insufficient data for analysis (need ≥5 rows)"
</INSTRUCTION>"""

    # Enhanced statistics with quality insights
    stats = _analyze_sample_data_enhanced(columns, sample_rows)

    # Format column summary with pre-computed insights
    column_summaries = []
    for col in columns:
        col_name = col.get("column_name", "unknown")
        col_type = col.get("column_type", "unknown")
        col_stat = stats.get(col_name, {})

        summary = f"  • {col_name} ({col_type})"
        insights = []

        # Null info
        null_count = col_stat.get("null_count", 0)
        null_pct = col_stat.get("null_pct", 0)
        if null_count > 0:
            insights.append(f"{null_pct:.0f}% null ({null_count}/{len(sample_rows)})")

        # Distinct values
        distinct = col_stat.get("distinct_count", 0)
        if 0 < distinct <= 10:
            values = col_stat.get("sample_values", [])
            insights.append(f"values: {', '.join(map(str, values))}")
        elif distinct > 0:
            insights.append(f"{distinct} distinct")

        # Range info
        if col_stat.get("min") is not None:
            insights.append(f"range: [{col_stat['min']}, {col_stat['max']}]")

        # Quality flags
        if col_stat.get("has_email_issues"):
            insights.append(f"⚠️ {col_stat['email_invalid_count']} invalid emails")
        if col_stat.get("has_duplicates"):
            insights.append(f"⚠️ {col_stat['duplicate_count']} duplicates")
        if col_stat.get("has_placeholder"):
            insights.append(f"⚠️ contains placeholder values")

        if insights:
            summary += f"\n    → {' | '.join(insights)}"

        column_summaries.append(summary)

    columns_text = "\n".join(column_summaries)

    # Format sample data
    import json

    sample_text = json.dumps(sample_rows[:10], indent=2, default=str)

    # Warning for limited samples
    sample_warning = ""
    if len(sample_rows) < 10:
        sample_warning = (
            "\n⚠️ CAUTION: Limited samples—only report glaring, obvious issues."
        )

    prompt = f"""<DATA>
TABLE: {schema_name}.{table_name}
SAMPLE SIZE: {len(sample_rows)} rows{sample_warning}

COLUMN SUMMARY (pre-computed insights):
{columns_text}

RAW SAMPLE DATA (scan every value):
{sample_text}
</DATA>

<TASK>
Think step-by-step:

STEP 1: SCAN EVERY VALUE
For each column, examine every value in the sample data above.

STEP 2: IDENTIFY PATTERNS
- Email columns: Count how many have @ symbol vs missing @
- Phone columns: Note format variations ("+XXX" vs "0X-XX-XX")
- Date columns: Check for future dates, format consistency
- Numeric columns: Check for negative values, unrealistic ranges
- Category columns: List distinct values, check for unexpected ones
- ID columns: Check for duplicates in the sample

STEP 3: QUANTIFY ISSUES
Calculate percentages: "X out of Y values have problem (Z%)"
Only report if >10% of non-null values show the issue OR if it's a critical field (ID, email)

STEP 4: DECIDE & OUTPUT
For each column with a real, quantified issue:
- Quote exact problematic values with row IDs
- State the observed pattern with numbers
- Provide specific validation rule (regex, constraint, enum)
- Assign severity (high=business critical, medium=quality improvement)

Skip columns that look fine after analysis.
</TASK>

<FOCUS_AREAS>
1. EMAIL columns (email, e_mail, mail):
   → Does EVERY value contain @? If not, count how many are invalid and quote examples

2. PHONE columns (telephone, phone, mobile):
   → Do all follow same format? If mixed, show examples of each format

3. DATE/TIMESTAMP columns:
   → Any future dates where illogical? Any format inconsistencies?

4. NUMERIC columns (age, price, amount, quantity):
   → Any negative where invalid? Any unrealistic (age>120)?

5. CATEGORY/STATUS columns:
   → List all distinct values. Any unexpected? Suggest enum if appropriate

6. ID/CODE columns:
   → Any duplicates in sample? Any format inconsistencies?

7. TEXT/ADDRESS columns:
   → Any placeholder values ("test", "dummy", "N/A")?

8. CROSS-COLUMN logic:
   → Any date ordering issues? Any contradictions?
</FOCUS_AREAS>

<OUTPUT_INSTRUCTION>
Based on your step-by-step analysis, output JSON array of ONLY columns with real problems.

Include:
- Exact values and row IDs
- Quantified patterns (X out of Y)
- Specific validation rules (regex/constraints)

If no problems found after thorough analysis, output exactly: []
</OUTPUT_INSTRUCTION>"""

    return prompt


def _analyze_sample_data_enhanced(columns: list, sample_rows: list) -> dict:
    """
    Enhanced analysis with automatic quality flag detection

    Args:
        columns: List of column metadata
        sample_rows: Sample data rows

    Returns:
        Dictionary with column stats and quality flags
    """
    if not sample_rows:
        return {}

    import re

    stats = {}
    sample_size = len(sample_rows)

    for col in columns:
        col_name = col.get("column_name")
        if not col_name:
            continue

        values = [row.get(col_name) for row in sample_rows]
        non_null_values = [v for v in values if v is not None and v != ""]

        null_count = len(values) - len(non_null_values)

        col_stats = {
            "null_count": null_count,
            "null_pct": (null_count / sample_size * 100) if sample_size > 0 else 0,
            "distinct_count": (
                len(set(str(v) for v in non_null_values)) if non_null_values else 0
            ),
        }

        # Capture low cardinality values
        if col_stats["distinct_count"] <= 10 and non_null_values:
            unique_vals = sorted(set(str(v) for v in non_null_values))[:10]
            col_stats["sample_values"] = unique_vals

        # Numeric range
        try:
            numeric_values = [float(v) for v in non_null_values if v is not None]
            if numeric_values:
                col_stats["min"] = min(numeric_values)
                col_stats["max"] = max(numeric_values)
        except (ValueError, TypeError):
            pass

        # EMAIL VALIDATION (auto-detect issues)
        if "email" in col_name.lower() or "mail" in col_name.lower():
            email_pattern = re.compile(
                r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"
            )
            invalid_emails = [
                v for v in non_null_values if not email_pattern.match(str(v))
            ]
            if invalid_emails:
                col_stats["has_email_issues"] = True
                col_stats["email_invalid_count"] = len(invalid_emails)
                col_stats["email_invalid_examples"] = invalid_emails[:3]

        # DUPLICATE DETECTION
        if (
            "id" in col_name.lower()
            or "_id" in col_name.lower()
            or "key" in col_name.lower()
        ):
            value_counts = {}
            for v in non_null_values:
                value_counts[str(v)] = value_counts.get(str(v), 0) + 1
            duplicates = {v: c for v, c in value_counts.items() if c > 1}
            if duplicates:
                col_stats["has_duplicates"] = True
                col_stats["duplicate_count"] = sum(duplicates.values()) - len(
                    duplicates
                )

        # PLACEHOLDER DETECTION
        placeholder_values = {
            "test",
            "dummy",
            "n/a",
            "na",
            "null",
            "xxx",
            "none",
            "invalid",
        }
        has_placeholder = any(
            str(v).lower() in placeholder_values for v in non_null_values
        )
        if has_placeholder:
            col_stats["has_placeholder"] = True

        stats[col_name] = col_stats

    return stats
