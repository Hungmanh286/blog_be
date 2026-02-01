"""
World Market Analysis Schemas
"""

from typing import Optional
from pydantic import BaseModel, Field


class WorldMarketAnalysisBase(BaseModel):
    """Base schema for World Market Analysis."""

    sector: str = Field(..., description="Ngành (Sector)")
    pe_percentile: Optional[float] = Field(None, description="PE percentile")
    pb_percentile: Optional[float] = Field(None, description="PB percentile")


class WorldMarketAnalysisResponse(WorldMarketAnalysisBase):
    """Response schema for World Market Analysis."""

    model_config = {
        "from_attributes": True,
        "json_schema_extra": {
            "example": {
                "sector": "Công nghệ thông tin",
                "pe_percentile": 75.5,
                "pb_percentile": 99.2,
            }
        },
    }


class WorldMarketAnalysisCreate(WorldMarketAnalysisBase):
    """Create schema for World Market Analysis."""

    pass
