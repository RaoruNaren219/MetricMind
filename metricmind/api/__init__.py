"""
FastAPI application and API endpoints for MetricMind.
"""

from .app import app
from .routes import router

__all__ = ['app', 'router'] 