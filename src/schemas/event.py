from datetime import datetime
from typing import Dict, Optional

from pydantic import BaseModel, Field


class UTMData(BaseModel):
    source: Optional[str] = None
    medium: Optional[str] = None
    campaign: Optional[str] = None


class DeviceData(BaseModel):
    ua: Optional[str] = None
    os: Optional[str] = None
    mobile: bool = False


class GeoData(BaseModel):
    country: Optional[str] = None
    city: Optional[str] = None


class ClickstreamEvent(BaseModel):
    event_id: str = Field(..., description="Unique event identifier")
    event_type: str = Field(..., description="Event type: page_view, add_to_cart, purchase")
    event_ts: str = Field(..., description="Event timestamp in RFC3339 format")
    user_id: str = Field(..., description="Pseudonymous user identifier")
    session_id: str = Field(..., description="Session identifier")
    page: str = Field(..., description="Page path")
    product_id: Optional[str] = Field(None, description="Product SKU for product-related events")
    price: Optional[float] = Field(None, description="Price for cart/purchase events")
    currency: Optional[str] = Field("USD", description="Currency code")
    qty: Optional[int] = Field(None, description="Quantity for cart/purchase events")
    referrer: Optional[str] = Field(None, description="Referrer URL")
    utm: UTMData = Field(default_factory=UTMData)
    device: DeviceData = Field(default_factory=DeviceData)
    geo: GeoData = Field(default_factory=GeoData)

    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }


class KPIMinute(BaseModel):
    minute: str
    sessions: int
    page_views: int
    add_to_carts: int
    purchases: int
    conversion_rate: float
    revenue: float
    aov: float
    latency_ms_p95: float


class TopProduct(BaseModel):
    product_id: str
    qty: int
    revenue: float


class TopProductsMinute(BaseModel):
    minute: str
    items: list[TopProduct]


class CampaignKPIsMinute(BaseModel):
    minute: str
    utm: UTMData
    sessions: int
    purchases: int
    revenue: float
    cr: float