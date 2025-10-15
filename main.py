from fastapi import FastAPI
from app.api.v1 import profiling_suggestion
from app.db.schema import init_db
from app.core.config import settings
from app.core.logging import logger
from contextlib import asynccontextmanager

# Initialize database
init_db()


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup code here (e.g., your logging init)
    logger.info("Starting LLM DQ Suggestions")
    yield
    # Shutdown code here if needed


# Create FastAPI app
app = FastAPI(title=settings.app_name, debug=settings.debug, lifespan=lifespan)

# Include routers
app.include_router(profiling_suggestion.router)


@app.get("/health")
def health_check():
    return {"status": "healthy"}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
