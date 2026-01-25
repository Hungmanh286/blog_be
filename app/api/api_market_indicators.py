"""
Market Indicators API
Endpoint for uploading Market Indicators Excel file.
"""

import logging
from fastapi import APIRouter, UploadFile, File, Depends, HTTPException
from sqlalchemy.orm import Session
from typing import Dict, Any

from app.db import get_db
from app.services.market_indicators_service import MarketIndicatorsService
from app.schemas.sche_base import DataResponse

logger = logging.getLogger(__name__)

router = APIRouter()


@router.post("/upload", response_model=DataResponse[Dict[str, Any]])
async def upload_market_indicators(
    file: UploadFile = File(..., description="Market Indicators Excel file (.xlsx)"),
    db: Session = Depends(get_db)
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
    """
    # Validate file type
    if not file.filename.endswith(('.xlsx', '.xls')):
        raise HTTPException(
            status_code=400,
            detail="Invalid file type. Please upload an Excel file (.xlsx or .xls)"
        )
    
    try:
        # Read file content
        content = await file.read()
        
        # Process the Excel file
        service = MarketIndicatorsService(db)
        result = service.process_excel_file(content, file.filename)
        
        # Check for errors
        if result['status'] == 'error':
            raise HTTPException(
                status_code=500,
                detail=f"Failed to process file: {result['message']}"
            )
        
        return DataResponse[Dict[str, Any]]().custom_response(
            code='000',
            message=result['message'],
            data=result
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error uploading market indicators: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Error processing file: {str(e)}"
        )


@router.get("/status", response_model=DataResponse[Dict[str, Any]])
async def get_market_indicators_status(db: Session = Depends(get_db)):
    """
    Get status of market_indicators table (row count and date range).
    """
    from sqlalchemy import text
    
    try:
        # Get row count
        result = db.execute(text("SELECT COUNT(*) FROM market_indicators"))
        count = result.scalar()
        
        # Get date range
        result = db.execute(text("""
            SELECT 
                MIN(report_date) as min_date,
                MAX(report_date) as max_date
            FROM market_indicators
        """))
        date_range = result.fetchone()
        
        status = {
            'table': 'market_indicators',
            'row_count': count,
            'date_range': {
                'from': str(date_range[0]) if date_range[0] else None,
                'to': str(date_range[1]) if date_range[1] else None
            }
        }
        
        return DataResponse[Dict[str, Any]]().success_response(data=status)
        
    except Exception as e:
        logger.error(f"Error getting status: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Error getting table status: {str(e)}"
        )


@router.get("/sample", response_model=DataResponse[list])
async def get_sample_data(
    limit: int = 10,
    db: Session = Depends(get_db)
):
    """
    Get sample data from market_indicators table.
    """
    from sqlalchemy import text
    
    try:
        result = db.execute(text(f"""
            SELECT * FROM market_indicators 
            ORDER BY report_date DESC 
            LIMIT {limit}
        """))
        
        # Convert to list of dicts
        columns = result.keys()
        rows = []
        for row in result:
            row_dict = {}
            for i, col in enumerate(columns):
                value = row[i]
                # Convert date to string
                if hasattr(value, 'isoformat'):
                    value = value.isoformat()
                row_dict[col] = value
            rows.append(row_dict)
        
        return DataResponse[list]().success_response(data=rows)
        
    except Exception as e:
        logger.error(f"Error getting sample data: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Error getting sample data: {str(e)}"
        )
