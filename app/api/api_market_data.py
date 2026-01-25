"""
Market Data API
Endpoint for uploading Market Watch Excel file.
"""

import logging
from fastapi import APIRouter, UploadFile, File, Depends, HTTPException
from sqlalchemy.orm import Session
from typing import Dict, Any

from app.db import get_db
from app.services.market_data_service import MarketDataService
from app.schemas.sche_base import DataResponse

logger = logging.getLogger(__name__)

router = APIRouter()


@router.post("/upload", response_model=DataResponse[Dict[str, Any]])
async def upload_market_data(
    file: UploadFile = File(..., description="Market Watch Excel file (.xlsx)"),
    db: Session = Depends(get_db)
):
    """
    Upload Market Watch Excel file and automatically insert data into corresponding tables.
    
    The Excel file should contain sheets with market data that will be mapped to:
    - market_indicators: Daily market indicators (Vol, Bre, Map, Div, Eps, PB, %St, Ret, Tur, Der, Ins, Tra, Avg, Mat, Cor, Hea)
    - portfolio_performance: Portfolio performance by year (Por)
    - sector_valuation: Sector valuation data (Sec)
    - world_market_analysis: World market analysis (Wma)
    - macro_indicators: Macro indicators (% GDP, Mar)
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
        service = MarketDataService(db)
        result = service.process_excel_file(content, file.filename)
        
        # Check for errors
        if result['errors'] and not result['sheets_processed']:
            raise HTTPException(
                status_code=500,
                detail=f"Failed to process file: {'; '.join(result['errors'])}"
            )
        
        return DataResponse[Dict[str, Any]]().custom_response(
            code='000' if not result['errors'] else '207',
            message=f"Đã xử lý {len(result['sheets_processed'])} sheets với {result['total_records_inserted']} records",
            data=result
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error uploading market data: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Error processing file: {str(e)}"
        )


@router.get("/tables-status", response_model=DataResponse[Dict[str, Any]])
async def get_tables_status(db: Session = Depends(get_db)):
    """
    Check the status of market data tables (row counts).
    """
    from sqlalchemy import text
    
    tables = [
        'market_indicators',
        'portfolio_performance', 
        'sector_valuation',
        'world_market_analysis',
        'macro_indicators'
    ]
    
    status = {}
    for table in tables:
        try:
            result = db.execute(text(f"SELECT COUNT(*) FROM {table}"))
            count = result.scalar()
            status[table] = {'exists': True, 'row_count': count}
        except Exception as e:
            status[table] = {'exists': False, 'error': str(e)}
    
    return DataResponse[Dict[str, Any]]().success_response(data=status)
