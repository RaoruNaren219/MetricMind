from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from metricmind.api.routes import router
from metricmind.utils.config import get_settings
from metricmind.utils.logger import setup_logger

# Initialize settings and logger
settings = get_settings()
logger = setup_logger()

# Create FastAPI app
app = FastAPI(
    title="MetricMind API",
    description="API for benchmarking Dremio instances using TPC-DS queries",
    version="1.0.0"
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # In production, replace with specific origins
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers
app.include_router(router, prefix="/api/v1")

@app.get("/")
async def root():
    """Root endpoint returning API information."""
    return {
        "name": "MetricMind API",
        "version": "1.0.0",
        "description": "API for benchmarking Dremio instances using TPC-DS queries"
    }

@app.on_event("startup")
async def startup_event():
    """Initialize resources on startup."""
    logger.info("Starting MetricMind API")
    # Add any initialization code here

@app.on_event("shutdown")
async def shutdown_event():
    """Cleanup resources on shutdown."""
    logger.info("Shutting down MetricMind API")
    # Add any cleanup code here 