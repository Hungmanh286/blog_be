"""
World Market Analysis API
Endpoints for uploading and retrieving world market analysis data.
"""

import logging
from typing import Dict, Any
from fastapi import APIRouter, UploadFile, File, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from app.db import get_db
from app.services.world_market_analysis import WorldMarketAnalysisService
from app.schemas.sche_base import DataResponse, MetadataSchema
from app.schemas.sche_world_market import WorldMarketAnalysisResponse

logger = logging.getLogger(__name__)

router = APIRouter()


@router.post("/upload", response_model=DataResponse[Dict[str, Any]])
async def upload_world_market_data(
    file: UploadFile = File(
        ..., description="World Market Analysis Excel file (.xlsx)"
    ),
    db: Session = Depends(get_db),
):
    """
    Upload World Market Analysis Excel file.

    The Excel file should contain the following columns:
    - Ngành (Sector): Tên ngành
    - PE percentile: PE percentile value
    - PB percentile: PB percentile value

    Example data:
    ```
    Ngành                   PE percentile  PB percentile
    Công nghệ thông tin     75.5           99.2
    Cao su                  53.1           81.3
    Dầu khí                 76.0           57.8
    ```

    All existing data will be replaced with the new data from the uploaded file.
    """
    # Validate file type
    if not file.filename.endswith((".xlsx", ".xls")):
        raise HTTPException(
            status_code=400,
            detail="Invalid file type. Please upload an Excel file (.xlsx or .xls)",
        )

    try:
        content = await file.read()

        service = WorldMarketAnalysisService(db)
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
        logger.error(f"Error uploading world market data: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Error processing file: {str(e)}")


@router.get("/", response_model=DataResponse[Dict[str, Any]])
def get_world_market_data(
    skip: int = Query(0, ge=0, description="Number of records to skip"),
    limit: int = Query(
        100, ge=1, le=1000, description="Maximum number of records to return"
    ),
    db: Session = Depends(get_db),
):
    """
    Retrieve all world market analysis data with pagination.

    Returns:
    - List of world market analysis records
    - Metadata with pagination information
    """
    try:
        service = WorldMarketAnalysisService(db)

        # Get data
        data = service.get_all_data(skip=skip, limit=limit)

        # Get total count
        total_count = service.get_total_count()

        # Convert to response format
        items = [WorldMarketAnalysisResponse.model_validate(record) for record in data]

        # Create metadata
        metadata = MetadataSchema(
            current_page=(skip // limit) + 1 if limit > 0 else 1,
            page_size=limit,
            total_items=total_count,
        )

        response_data = {
            "items": [item.model_dump() for item in items],
            "metadata": metadata.model_dump(),
        }

        return DataResponse[Dict[str, Any]]().success_response(data=response_data)

    except Exception as e:
        logger.error(f"Error retrieving world market data: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Error retrieving data: {str(e)}")


@router.get("/{sector}", response_model=DataResponse[WorldMarketAnalysisResponse])
def get_world_market_by_sector(
    sector: str,
    db: Session = Depends(get_db),
):
    """
    Retrieve world market analysis data by sector.

    Args:
        sector: Sector name (e.g., "Công nghệ thông tin")

    Returns:
        World market analysis data for the specified sector
    """
    try:
        service = WorldMarketAnalysisService(db)
        data = service.get_by_sector(sector)

        if not data:
            raise HTTPException(
                status_code=404, detail=f"Không tìm thấy dữ liệu cho ngành '{sector}'"
            )

        response = WorldMarketAnalysisResponse.model_validate(data)

        return DataResponse[WorldMarketAnalysisResponse]().success_response(
            data=response
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error retrieving sector data: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Error retrieving data: {str(e)}")
