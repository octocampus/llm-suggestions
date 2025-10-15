# LLM DQ Suggestions

LLM-powered data quality suggestions based on profiling data.

## Architecture

```
app/
├── api/v1/          # API routes (thin layer)
├── core/            # Config, logging
├── db/              # Database models & setup
├── model/           # Pydantic schemas
└── services/        # Business logic
```

## Setup

### Using UV (Recommended)

```bash
# Install dependencies
uv pip install -r pyproject.toml

# Run the app
python main.py
```

### Using Docker

```bash
# Build and run
docker-compose up --build

# API will be available at http://localhost:8000
```

## Usage

### Fetch and store profiling data

```bash
curl -X POST "http://localhost:8000/api/v1/profiling/fetch?source_key=nemo_telecom_data&schema_name=billing_finance_space&table_name=billing_transactions"
```

### Query stored profiling data

```bash
# Get all profiles
curl "http://localhost:8000/api/v1/profiling/"

# Filter by table
curl "http://localhost:8000/api/v1/profiling/?table_name=billing_transactions"
```

## API Documentation

Visit http://localhost:8000/docs for interactive API documentation.

## Environment Variables

Create a `.env` file:

```bash
DATABASE_URL=sqlite:///./test.db
EXTERNAL_API_BASE_URL=http://localhost:8085/api/profiling
LOG_LEVEL=INFO
```

## Testing

```bash
pytest
```

