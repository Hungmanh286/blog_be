"""
Market Indicators API
Endpoint for uploading Market Indicators Excel file.
"""

import logging
from fastapi import APIRouter, UploadFile, File, Depends, HTTPException
from sqlalchemy.orm import Session
from typing import Dict, Any
from sqlalchemy import text
from collections import defaultdict

from app.db.models import MarketIndicators
from app.db import get_db
from app.services.market_indicators_service import MarketIndicatorsService
from app.schemas.sche_base import DataResponse

logger = logging.getLogger(__name__)

router = APIRouter()


@router.post("/upload", response_model=DataResponse[Dict[str, Any]])
async def upload_market_indicators(
    file: UploadFile = File(..., description="Market Indicators Excel file (.xlsx)"),
    db: Session = Depends(get_db),
):
    """
    Upload Market Indicators Excel file and merge all sheets by date.

    The Excel file should contain multiple sheets, each with:
    - A Date column (or Unnamed: 0 for Ret and %St sheets)
    - One or more indicator columns

    All sheets will be merged by date and inserted into the market_indicators table.

    Expected sheets:
    - Vol: Volatility Index
    - Bre: Breadth Index
    - Map: Market price
    - Div: Dividend 12M %
    - Eps: VNIndex & Nonbank EPS
    - %St: PE < 10%, PB < 1%
    - PB: VNIndex, Non bank, Bank PB
    - Ret: 40 Years Old, VNI Adjusted returns
    - Tur: Turnover ratio
    - Der: Derivatives ratio
    - Ins: Insider Transaction 3M %
    - Tra: Probability
    - Avg: Avg 50D Orders
    - Mat: Matching Rate %
    - Cor: SPX, VN1Y, USD correlations
    - Hea: Consumption, Production, Labor health
    - %GDP: % GDP, % M2
    - Mar: Margin, Deposit
    """
    # Validate file type
    if not file.filename.endswith((".xlsx", ".xls")):
        raise HTTPException(
            status_code=400,
            detail="Invalid file type. Please upload an Excel file (.xlsx or .xls)",
        )

    try:
        content = await file.read()

        service = MarketIndicatorsService(db)
        result = service.process_excel_file(content, file.filename)

        if result["status"] == "error":
            raise HTTPException(
                status_code=500, detail=f"Failed to process file: {result['message']}"
            )

        return DataResponse[Dict[str, Any]]().custom_response(
            code="000", message=result["message"], data=result
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error uploading market indicators: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Error processing file: {str(e)}")


@router.get("/status", response_model=DataResponse[Dict[str, Any]])
async def get_market_indicators_status(db: Session = Depends(get_db)):
    """
    Get status of market_indicators table (row count and date range).
    """

    try:
        result = db.execute(text("SELECT COUNT(*) FROM market_indicators"))
        count = result.scalar()

        result = db.execute(
            text("""
            SELECT 
                MIN(report_date) as min_date,
                MAX(report_date) as max_date
            FROM market_indicators
        """)
        )
        date_range = result.fetchone()

        status = {
            "table": "market_indicators",
            "row_count": count,
            "date_range": {
                "from": str(date_range[0]) if date_range[0] else None,
                "to": str(date_range[1]) if date_range[1] else None,
            },
        }

        return DataResponse[Dict[str, Any]]().success_response(data=status)

    except Exception as e:
        logger.error(f"Error getting status: {e}", exc_info=True)
        raise HTTPException(
            status_code=500, detail=f"Error getting table status: {str(e)}"
        )


@router.get("", response_model=DataResponse[Dict[str, Any]])
async def get_market_indicators_data(
    limit: int = 10, indicators: str = None, db: Session = Depends(get_db)
):
    """
    Get sample data from market_indicators table.
    Returns a dictionary where each key is an indicator name and value is a list of date-value pairs.

    Parameters:
    - limit: Number of recent records to fetch (default: 10)
    - indicators: Comma-separated list of indicator names to fetch. If not provided, all indicators are returned.
                 Example: "volatility_index,breadth_index,market_price"

    Example response: {
        "volatility_index": [
            { "date": "2026-01-22", "value": 1.7 },
            { "date": "2026-01-21", "value": 1.8 }
        ]
    }
    """

    try:
        valid_columns = {
            c.name
            for c in MarketIndicators.__table__.columns
            if c.name != "report_date"
        }

        requested_indicators = None
        if indicators:
            requested_indicators = set(ind.strip() for ind in indicators.split(","))

        cols_to_fetch = (
            list(requested_indicators) if requested_indicators else list(valid_columns)
        )

        indicators_dict = defaultdict(list)

        for col in cols_to_fetch:
            if col not in valid_columns:
                continue

            query = text(
                f"""
                SELECT report_date, {col} 
                FROM market_indicators 
                WHERE {col} IS NOT NULL 
                ORDER BY report_date DESC 
                LIMIT :limit
            """
            )

            result = db.execute(query, {"limit": limit})
            for row in result:
                report_date = row[0]
                val = row[1]

                if report_date is not None:
                    report_date_str = (
                        report_date.isoformat()
                        if hasattr(report_date, "isoformat")
                        else str(report_date)
                    )

                    if hasattr(val, "isoformat"):
                        val = val.isoformat()

                    indicators_dict[col].append({"date": report_date_str, "value": val})

        return DataResponse[Dict[str, Any]]().success_response(
            data=dict(indicators_dict)
        )

    except Exception as e:
        logger.error(f"Error getting sample data: {e}", exc_info=True)
        raise HTTPException(
            status_code=500, detail=f"Error getting sample data: {str(e)}"
        )
