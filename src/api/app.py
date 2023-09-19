#!/usr/bin/env python3
"""FastAPI read endpoints for analytics data."""

import os
from datetime import datetime, timedelta
from typing import List, Optional

from fastapi import FastAPI, Query, HTTPException
from pydantic import BaseModel
from pymongo import MongoClient
import uvicorn


# Configuration
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB = "analytics"
MONGO_COLLECTION = "kpis_minute"

app = FastAPI(
    title="E-commerce Analytics API",
    description="Real-time analytics data API",
    version="1.0.0"
)


# Response models
class KPIResponse(BaseModel):
    minute: str
    sessions: int
    page_views: int
    add_to_carts: int
    purchases: int
    conversion_rate: float
    revenue: float
    aov: float
    latency_ms_p95: float


class FunnelResponse(BaseModel):
    page_views: int
    add_to_carts: int
    purchases: int
    view_to_cart_rate: float
    cart_to_purchase_rate: float
    overall_conversion_rate: float


@app.get("/")
async def root():
    """Health check endpoint."""
    return {"message": "E-commerce Analytics API", "status": "healthy"}


@app.get("/kpis", response_model=List[KPIResponse])
async def get_kpis(
    minutes_back: int = Query(30, description="Minutes of historical data"),
    limit: int = Query(100, description="Maximum number of records")
):
    """Get KPI data for the specified time window."""
    
    try:
        client = MongoClient(MONGO_URI)
        db = client[MONGO_DB]
        collection = db[MONGO_COLLECTION]
        
        # Calculate cutoff time
        cutoff_time = datetime.utcnow() - timedelta(minutes=minutes_back)
        
        # Query data
        cursor = collection.find(
            {"minute": {"$gte": cutoff_time.isoformat()}},
            sort=[("minute", -1)],
            limit=limit
        )
        
        data = list(cursor)
        client.close()
        
        return [KPIResponse(**doc) for doc in data]
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")


@app.get("/kpis/latest", response_model=KPIResponse)
async def get_latest_kpis():
    """Get the most recent KPI data point."""
    
    try:
        client = MongoClient(MONGO_URI)
        db = client[MONGO_DB]
        collection = db[MONGO_COLLECTION]
        
        # Get latest record
        doc = collection.find_one(sort=[("minute", -1)])
        client.close()
        
        if not doc:
            raise HTTPException(status_code=404, detail="No data found")
        
        return KPIResponse(**doc)
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")


@app.get("/funnel", response_model=FunnelResponse)
async def get_funnel(
    minutes_back: int = Query(5, description="Minutes to aggregate for funnel")
):
    """Get conversion funnel data for the specified time window."""
    
    try:
        client = MongoClient(MONGO_URI)
        db = client[MONGO_DB]
        collection = db[MONGO_COLLECTION]
        
        # Calculate cutoff time
        cutoff_time = datetime.utcnow() - timedelta(minutes=minutes_back)
        
        # Aggregate funnel data
        pipeline = [
            {"$match": {"minute": {"$gte": cutoff_time.isoformat()}}},
            {"$group": {
                "_id": None,
                "total_page_views": {"$sum": "$page_views"},
                "total_add_to_carts": {"$sum": "$add_to_carts"},
                "total_purchases": {"$sum": "$purchases"}
            }}
        ]
        
        result = list(collection.aggregate(pipeline))
        client.close()
        
        if not result:
            raise HTTPException(status_code=404, detail="No funnel data found")
        
        data = result[0]
        page_views = data.get("total_page_views", 0)
        add_to_carts = data.get("total_add_to_carts", 0)
        purchases = data.get("total_purchases", 0)
        
        # Calculate rates
        view_to_cart_rate = add_to_carts / page_views if page_views > 0 else 0
        cart_to_purchase_rate = purchases / add_to_carts if add_to_carts > 0 else 0
        overall_conversion_rate = purchases / page_views if page_views > 0 else 0
        
        return FunnelResponse(
            page_views=page_views,
            add_to_carts=add_to_carts,
            purchases=purchases,
            view_to_cart_rate=view_to_cart_rate,
            cart_to_purchase_rate=cart_to_purchase_rate,
            overall_conversion_rate=overall_conversion_rate
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")


@app.get("/health")
async def health_check():
    """Detailed health check including database connectivity."""
    
    try:
        client = MongoClient(MONGO_URI)
        db = client[MONGO_DB]
        
        # Test database connection
        server_info = client.server_info()
        
        # Check if we have recent data
        collection = db[MONGO_COLLECTION]
        latest_doc = collection.find_one(sort=[("minute", -1)])
        
        client.close()
        
        data_freshness = None
        if latest_doc:
            latest_time = datetime.fromisoformat(latest_doc['minute'].replace('Z', '+00:00'))
            data_age = (datetime.utcnow() - latest_time.replace(tzinfo=None)).total_seconds()
            data_freshness = f"{data_age:.0f} seconds ago"
        
        return {
            "status": "healthy",
            "database": "connected",
            "mongodb_version": server_info.get("version"),
            "latest_data": data_freshness,
            "timestamp": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        return {
            "status": "unhealthy",
            "error": str(e),
            "timestamp": datetime.utcnow().isoformat()
        }


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)

# Updated: 2025-10-04 19:46:28
# Added during commit replay
