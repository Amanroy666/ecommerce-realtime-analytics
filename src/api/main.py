"""
FastAPI Application for E-Commerce Analytics
Provides REST API for querying metrics and insights
"""
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional, List
from datetime import date, datetime
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="E-Commerce Analytics API",
    description="Real-time analytics API for e-commerce platform",
    version="1.0.0"
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

class MetricsQuery(BaseModel):
    store_id: str
    start_date: date
    end_date: date
    metrics: Optional[List[str]] = ["revenue", "transactions", "customers"]

class MetricsResponse(BaseModel):
    store_id: str
    period: str
    revenue: float
    transaction_count: int
    unique_customers: int
    avg_order_value: float

@app.get("/")
def root():
    return {"message": "E-Commerce Analytics API", "status": "healthy"}

@app.get("/health")
def health_check():
    return {
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "service": "analytics-api"
    }

@app.post("/api/v1/metrics", response_model=MetricsResponse)
def get_metrics(query: MetricsQuery):
    """Get aggregated metrics for a store"""
    logger.info(f"Fetching metrics for store {query.store_id}")
    
    # Query database (mock response for now)
    return MetricsResponse(
        store_id=query.store_id,
        period=f"{query.start_date} to {query.end_date}",
        revenue=1250000.00,
        transaction_count=15234,
        unique_customers=8945,
        avg_order_value=82.03
    )

@app.get("/api/v1/stores/{store_id}/performance")
def get_store_performance(
    store_id: str,
    days: int = Query(7, ge=1, le=90)
):
    """Get store performance for last N days"""
    return {
        "store_id": store_id,
        "days": days,
        "revenue_trend": [125000, 132000, 128500, 135000, 142000, 138000, 145000],
        "growth_rate": 3.2
    }

@app.get("/api/v1/realtime/dashboard")
def get_realtime_dashboard():
    """Get real-time dashboard metrics"""
    return {
        "current_active_users": 1245,
        "transactions_per_minute": 87,
        "revenue_per_minute": 7234.50,
        "top_selling_products": [
            {"product_id": "P123", "name": "Product A", "sales": 45},
            {"product_id": "P456", "name": "Product B", "sales": 38}
        ]
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
