# Quick Start Guide

## 🚀 Get Started in 3 Steps

### 1. Install Dependencies

```bash
# Using UV (faster)
uv pip install -r pyproject.toml

# OR using pip
pip install fastapi uvicorn sqlalchemy pydantic pydantic-settings httpx
```

### 2. Run the API

```bash
python main.py
```

The API will start on http://localhost:8000

### 3. Test It

Open another terminal and run:

```bash
python test_api.py
```

## 📝 Manual Testing

### Fetch and Save Profiling Data

```bash
curl -X POST "http://localhost:8000/api/v1/profiling/fetch?source_key=nemo_telecom_data&schema_name=billing_finance_space&table_name=billing_transactions"
```

### Get All Stored Profiles

```bash
curl "http://localhost:8000/api/v1/profiling/"
```

### Filter by Table Name

```bash
curl "http://localhost:8000/api/v1/profiling/?table_name=billing_transactions"
```

### Get Specific Profile

```bash
curl "http://localhost:8000/api/v1/profiling/f9b858ff-4999-416e-b97a-1051c4303f75"
```

## 📚 Interactive API Docs

Visit http://localhost:8000/docs

## 🐳 Using Docker

```bash
# Build and run
docker-compose up --build

# In another terminal, test
curl "http://localhost:8000/health"
```

## ⚙️ Configuration

Edit `.env` file:

```bash
EXTERNAL_API_BASE_URL=http://localhost:8085/api/profiling
DATABASE_URL=sqlite:///./test.db
LOG_LEVEL=INFO
```

## 📊 What This Does

1. **Fetches** profiling data from your external API (port 8085)
2. **Stores** it in SQLite database
3. **Provides** REST API to query stored data
4. **Ready** for adding LLM suggestions later

## 🔍 Architecture

```
┌─────────────────┐
│  External API   │  (Port 8085)
│  (Profiling)    │
└────────┬────────┘
         │ Fetch JSON
         ▼
┌─────────────────┐
│   Your API      │  (Port 8000)
│  (FastAPI)      │
└────────┬────────┘
         │ Save
         ▼
┌─────────────────┐
│   SQLite DB     │
│  (test.db)      │
└─────────────────┘
```

## ✅ Verify Everything Works

```bash
# 1. Start the API
python main.py

# 2. Check health
curl http://localhost:8000/health

# 3. Fetch profiling data
curl -X POST "http://localhost:8000/api/v1/profiling/fetch?source_key=nemo_telecom_data&schema_name=billing_finance_space&table_name=billing_transactions"

# 4. View stored data
curl http://localhost:8000/api/v1/profiling/
```

That's it! 🎉

