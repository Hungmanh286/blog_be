# filepath: /home/hungmanh/Documents/data_blog_be/fastapi-base/app/services/world_market_analysis.py
"""
World Market Analysis Service
Handles Excel file upload and retrieval of world market analysis data.
"""

import io
import logging
from typing import Dict, Any, List

import pandas as pd
from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError

from app.db.models import WorldMarketAnalysis

logger = logging.getLogger(__name__)


class WorldMarketAnalysisService:
    """Service for handling world market analysis operations."""

    def __init__(self, db: Session):
        self.db = db

    def process_excel_file(self, file_content: bytes, filename: str) -> Dict[str, Any]:
        """
        Process uploaded Excel file and insert data into world_market_analysis table.

        Expected columns:
        - Ngành (Sector)
        - PE percentile
        - PB percentile

        Args:
            file_content: Excel file content as bytes
            filename: Original filename

        Returns:
            Dictionary with status and message
        """
        try:
            # Read Excel file
            df = pd.read_excel(io.BytesIO(file_content))

            # Log the columns for debugging
            logger.info(f"Excel columns: {df.columns.tolist()}")

            # Normalize column names (remove leading/trailing spaces)
            df.columns = df.columns.str.strip()

            # Expected column names (try multiple variations)
            sector_cols = ["Ngành", "nganh", "Sector", "sector"]
            pe_cols = ["PE percentile", "PE Percentile", "pe_percentile", "PE"]
            pb_cols = ["PB percentile", "PB Percentile", "pb_percentile", "PB"]

            # Find actual column names
            sector_col = None
            pe_col = None
            pb_col = None

            for col in df.columns:
                if col in sector_cols:
                    sector_col = col
                elif col in pe_cols:
                    pe_col = col
                elif col in pb_cols:
                    pb_col = col

            if not sector_col:
                return {
                    "status": "error",
                    "message": f"Không tìm thấy cột 'Ngành'. Các cột có sẵn: {df.columns.tolist()}",
                }

            # Prepare data for insertion
            records = []
            for _, row in df.iterrows():
                sector = row.get(sector_col)

                # Skip if sector is empty or NaN
                if pd.isna(sector) or str(sector).strip() == "":
                    continue

                pe_percentile = row.get(pe_col) if pe_col else None
                pb_percentile = row.get(pb_col) if pb_col else None

                # Convert to float if not None and not NaN
                if pe_percentile is not None and not pd.isna(pe_percentile):
                    pe_percentile = float(pe_percentile)
                else:
                    pe_percentile = None

                if pb_percentile is not None and not pd.isna(pb_percentile):
                    pb_percentile = float(pb_percentile)
                else:
                    pb_percentile = None

                records.append(
                    {
                        "sector": str(sector).strip(),
                        "pe_percentile": pe_percentile,
                        "pb_percentile": pb_percentile,
                    }
                )

            if not records:
                return {
                    "status": "error",
                    "message": "Không có dữ liệu hợp lệ để import",
                }

            # Delete existing data
            self.db.query(WorldMarketAnalysis).delete()

            # Insert new data
            for record in records:
                wma = WorldMarketAnalysis(**record)
                self.db.add(wma)

            self.db.commit()

            logger.info(f"Successfully imported {len(records)} records from {filename}")

            return {
                "status": "success",
                "message": f"Import thành công {len(records)} bản ghi",
                "records_imported": len(records),
                "filename": filename,
            }

        except pd.errors.ParserError as e:
            logger.error(f"Error parsing Excel file: {e}")
            return {"status": "error", "message": f"Lỗi đọc file Excel: {str(e)}"}
        except SQLAlchemyError as e:
            logger.error(f"Database error: {e}")
            self.db.rollback()
            return {"status": "error", "message": f"Lỗi database: {str(e)}"}
        except Exception as e:
            logger.error(f"Unexpected error: {e}", exc_info=True)
            self.db.rollback()
            return {"status": "error", "message": f"Lỗi không xác định: {str(e)}"}

    def get_all_data(
        self, skip: int = 0, limit: int = 100
    ) -> List[WorldMarketAnalysis]:
        """
        Retrieve all world market analysis data with pagination.

        Args:
            skip: Number of records to skip
            limit: Maximum number of records to return

        Returns:
            List of WorldMarketAnalysis records
        """
        try:
            return self.db.query(WorldMarketAnalysis).offset(skip).limit(limit).all()
        except SQLAlchemyError as e:
            logger.error(f"Error retrieving data: {e}")
            raise

    def get_total_count(self) -> int:
        """
        Get total count of records in world_market_analysis table.

        Returns:
            Total count of records
        """
        try:
            return self.db.query(WorldMarketAnalysis).count()
        except SQLAlchemyError as e:
            logger.error(f"Error counting records: {e}")
            raise

    def get_by_sector(self, sector: str) -> WorldMarketAnalysis:
        """
        Get world market analysis data by sector.

        Args:
            sector: Sector name

        Returns:
            WorldMarketAnalysis record or None
        """
        try:
            return (
                self.db.query(WorldMarketAnalysis)
                .filter(WorldMarketAnalysis.sector == sector)
                .first()
            )
        except SQLAlchemyError as e:
            logger.error(f"Error retrieving sector data: {e}")
            raise
