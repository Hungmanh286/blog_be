"""
Market Data Service
Handles Excel file upload and data insertion to corresponding tables.
"""

import io
import logging
from datetime import datetime
from typing import Dict, Any
from decimal import Decimal

import pandas as pd
from sqlalchemy.orm import Session
from sqlalchemy import text

logger = logging.getLogger(__name__)


# Mapping from Excel sheet to processing config
# Schema mới: Market_Indicators, Portfolio_Performance, Sector_Valuation,
#             World_Market_Analysis, Macro_Indicators

SHEET_CONFIG = {
    # === MARKET_INDICATORS (Dữ liệu thị trường hàng ngày) ===
    "Vol": {
        "table": "market_indicators",
        "date_col": "Date",
        "mapping": {"Volatility Index": "volatility_index"},
    },
    "Bre": {
        "table": "market_indicators",
        "date_col": "Date",
        "mapping": {"Breadth Index": "breadth_index"},
    },
    "Map": {
        "table": "market_indicators",
        "date_col": "Date",
        "mapping": {"Market price": "market_price"},
    },
    "Div": {
        "table": "market_indicators",
        "date_col": "Date",
        "mapping": {"% Dividen 12M": "dividend_12m_pct"},
    },
    "Eps": {
        "table": "market_indicators",
        "date_col": "Date",
        "mapping": {"Vnindex": "eps_vnindex", "Nonbank": "eps_nonbank"},
    },
    "PB": {
        "table": "market_indicators",
        "date_col": "Date",
        "mapping": {
            "Vnindex": "pb_vnindex",
            "Non bank": "pb_nonbank",
            "Bank": "pb_bank",
        },
    },
    "%St": {
        "table": "market_indicators",
        "date_col": "Date",
        "mapping": {"% PE < 10": "st_pe_under_10_pct", "% PB < 1": "st_pb_under_1_pct"},
    },
    "Ret": {
        "table": "market_indicators",
        "date_col": "Date",
        "mapping": {
            "40 Years Old": "ret_40years_old_pct",
            "VNI Adjusted": "ret_vni_adjusted_pct",
        },
    },
    "Tur": {
        "table": "market_indicators",
        "date_col": "Date",
        "mapping": {"Turnover ratio": "turnover_ratio"},
    },
    "Der": {
        "table": "market_indicators",
        "date_col": "Date",
        "mapping": {"Derivaties ratio": "derivatives_ratio"},
    },
    "Ins": {
        "table": "market_indicators",
        "date_col": "Date",
        "mapping": {"% Insider Transaction 3M": "insider_transaction_3m_pct"},
    },
    "Tra": {
        "table": "market_indicators",
        "date_col": "Date",
        "mapping": {"Probability": "probability"},
    },
    "Avg": {
        "table": "market_indicators",
        "date_col": "Date",
        "mapping": {"Avg 50D Orders": "avg_50d_orders"},
    },
    "Mat": {
        "table": "market_indicators",
        "date_col": "Date",
        "mapping": {"Matching Rate (%)": "matching_rate_pct"},
    },
    "Cor": {
        "table": "market_indicators",
        "date_col": "Date",
        "mapping": {"SPX": "cor_spx", "VN1Y": "cor_vn1y", "USD": "cor_usd"},
    },
    "Hea": {
        "table": "market_indicators",
        "date_col": "Date",
        "mapping": {
            "Consumption": "hea_consumption",
            "Production": "hea_production",
            "Labor": "hea_labor",
        },
    },
    "Por": {"table": "portfolio_performance", "year_col": "Year"},
    "Sec": {
        "table": "sector_valuation",
        "entity_col": "Ngành",
        "mapping": {"PE percentile": "pe_percentile", "PB percentile": "pb_percentile"},
    },
    # === WORLD_MARKET_ANALYSIS (Phân tích thị trường thế giới) ===
    "Wma": {
        "table": "world_market_analysis",
        "entity_col": "Unnamed: 0",
        "mapping": {
            "YTD (%)": "ytd_pct",
            "Percentile Growth ": "percentile_growth",
            "Percentile Valuation": "percentile_valuation",
        },
    },
    # === MACRO_INDICATORS (Chỉ số vĩ mô theo tháng) ===
    "% GDP": {
        "table": "macro_indicators",
        "date_col": " vb",
        "mapping": {"% GDP": "gdp_growth_pct", "% M2": "m2_growth_pct"},
    },
    "Mar": {
        "table": "macro_indicators",
        "date_col": "Unnamed: 0",
        "mapping": {"Margin": "mar_margin", "Deposit": "mar_deposit"},
    },
}

# Sheets that are not mapped (can be extended)
SKIP_SHEETS = []


class MarketDataService:
    """Service to process and import market data from Excel files."""

    def __init__(self, db: Session):
        self.db = db

    def process_excel_file(self, file_content: bytes, filename: str) -> Dict[str, Any]:
        """
        Process the entire Excel file and insert data to corresponding tables.

        Returns:
            Dictionary containing processing results for each sheet.
        """
        results = {
            "filename": filename,
            "sheets_processed": [],
            "sheets_skipped": [],
            "errors": [],
            "total_records_inserted": 0,
        }

        try:
            excel_file = pd.ExcelFile(io.BytesIO(file_content))
            logger.info(
                f"Processing Excel file: {filename} with sheets: {excel_file.sheet_names}"
            )

            for sheet_name in excel_file.sheet_names:
                if sheet_name in SKIP_SHEETS:
                    results["sheets_skipped"].append(sheet_name)
                    continue

                if sheet_name not in SHEET_CONFIG:
                    results["sheets_skipped"].append(sheet_name)
                    logger.warning(f"Sheet '{sheet_name}' not in config, skipping")
                    continue

                try:
                    df = pd.read_excel(excel_file, sheet_name)
                    config = SHEET_CONFIG[sheet_name]

                    logger.info(
                        f"Processing sheet '{sheet_name}' -> table '{config['table']}', rows: {len(df)}"
                    )

                    records_count = self._process_sheet(sheet_name, df, config)

                    # Commit after each sheet to avoid transaction issues
                    self.db.commit()

                    results["sheets_processed"].append(
                        {
                            "sheet": sheet_name,
                            "table": config["table"],
                            "records": records_count,
                        }
                    )
                    results["total_records_inserted"] += records_count
                    logger.info(
                        f"Sheet '{sheet_name}' processed successfully: {records_count} records"
                    )

                except Exception as e:
                    # Rollback the failed transaction and continue with next sheet
                    self.db.rollback()
                    error_msg = f"Error processing sheet '{sheet_name}': {str(e)}"
                    logger.error(error_msg, exc_info=True)
                    results["errors"].append(error_msg)

        except Exception as e:
            self.db.rollback()
            error_msg = f"Error reading Excel file: {str(e)}"
            logger.error(error_msg, exc_info=True)
            results["errors"].append(error_msg)

        return results

    def _process_sheet(self, sheet_name: str, df: pd.DataFrame, config: Dict) -> int:
        """Process a single sheet and insert data to the corresponding table."""
        table_name = config["table"]

        if table_name == "market_indicators":
            return self._process_market_indicators(df, config)
        elif table_name == "portfolio_performance":
            return self._process_portfolio_performance(df, config)
        elif table_name == "sector_valuation":
            return self._process_sector_valuation(df, config)
        elif table_name == "world_market_analysis":
            return self._process_world_market_analysis(df, config)
        elif table_name == "macro_indicators":
            return self._process_macro_indicators(df, config)

        return 0

    def _process_market_indicators(self, df: pd.DataFrame, config: Dict) -> int:
        """Insert/update data into market_indicators table using batch upsert."""
        date_col = config.get("date_col")
        mapping = config.get("mapping", {})

        if not date_col:
            logger.warning("No date_col configured for market_indicators sheet")
            return 0

        records = []
        for _, row in df.iterrows():
            try:
                date_value = row.get(date_col)
                if pd.isna(date_value):
                    continue
                report_date = pd.to_datetime(date_value).date()

                # Build update values from mapping
                update_values = {"report_date": report_date}
                for excel_col, db_col in mapping.items():
                    value = row.get(excel_col)
                    if pd.notna(value):
                        if isinstance(value, (int, float, Decimal)):
                            update_values[db_col] = float(value)
                        else:
                            update_values[db_col] = value

                if len(update_values) > 1:  # More than just report_date
                    records.append(update_values)

            except Exception as e:
                logger.debug(f"Skipping row due to error: {e}")
                continue

        # Batch upsert
        count = 0
        for record in records:
            try:
                report_date = record.pop("report_date")
                self._upsert_market_indicators(report_date, record)
                count += 1
            except Exception as e:
                logger.warning(f"Error upserting market indicators record: {e}")
                self.db.rollback()
                continue

        return count

    def _upsert_market_indicators(self, report_date, update_values: Dict):
        """Upsert a single record into market_indicators using PostgreSQL ON CONFLICT."""
        columns = ["report_date"] + list(update_values.keys())
        values_placeholders = ", ".join([f":{c}" for c in columns])

        # Build the update clause for ON CONFLICT
        update_clause = ", ".join([f"{k} = EXCLUDED.{k}" for k in update_values.keys()])

        sql = f"""
            INSERT INTO market_indicators ({", ".join(columns)}) 
            VALUES ({values_placeholders})
            ON CONFLICT (report_date) 
            DO UPDATE SET {update_clause}
        """

        params = {"report_date": report_date, **update_values}
        self.db.execute(text(sql), params)

    def _process_portfolio_performance(self, df: pd.DataFrame, config: Dict) -> int:
        """Insert/update data into portfolio_performance table."""
        # Giả sử cấu trúc: Cột 1 là Year, cột 2 là 40 Years Old, cột 3 là VNI Adjusted
        # Hoặc có thể là: Row header là portfolio name, columns là years

        # Ví dụ đơn giản: mỗi dòng là một năm với 2 portfolio
        portfolio_name_map = {
            "40 Years Old": "por_40_years_old",
            "VNI Adjusted": "por_vni_adjusted",
        }

        count = 0
        for _, row in df.iterrows():
            try:
                # Giả sử first column là Year
                year_value = row.iloc[0] if len(row) > 0 else None
                if pd.isna(year_value):
                    continue

                year = int(year_value)

                values = {}
                for col_name, db_col in portfolio_name_map.items():
                    val = row.get(col_name)
                    if pd.notna(val):
                        values[db_col] = float(val)

                if not values:
                    continue

                # Use PostgreSQL ON CONFLICT for upsert
                columns = ["year"] + list(values.keys())
                values_placeholders = ", ".join([f":{c}" for c in columns])
                update_clause = ", ".join(
                    [f"{k} = EXCLUDED.{k}" for k in values.keys()]
                )

                sql = f"""
                    INSERT INTO portfolio_performance ({", ".join(columns)}) 
                    VALUES ({values_placeholders})
                    ON CONFLICT (year) 
                    DO UPDATE SET {update_clause}
                """

                params = {"year": year, **values}
                self.db.execute(text(sql), params)
                count += 1

            except Exception as e:
                logger.warning(f"Error processing portfolio performance row: {e}")
                continue

        return count

    def _process_sector_valuation(self, df: pd.DataFrame, config: Dict) -> int:
        """Insert/update data into sector_valuation table."""
        entity_col = config["entity_col"]
        mapping = config["mapping"]

        count = 0
        for _, row in df.iterrows():
            try:
                sector_name = str(row[entity_col]).strip()
                if not sector_name or sector_name == "nan":
                    continue

                update_values = {}
                for excel_col, db_col in mapping.items():
                    value = row.get(excel_col)
                    if pd.notna(value):
                        update_values[db_col] = float(value)

                if not update_values:
                    continue

                # Use PostgreSQL ON CONFLICT for upsert
                columns = ["sector_name"] + list(update_values.keys())
                values_placeholders = ", ".join([f":{c}" for c in columns])
                update_clause = ", ".join(
                    [f"{k} = EXCLUDED.{k}" for k in update_values.keys()]
                )

                sql = f"""
                    INSERT INTO sector_valuation ({", ".join(columns)}) 
                    VALUES ({values_placeholders})
                    ON CONFLICT (sector_name) 
                    DO UPDATE SET {update_clause}
                """

                params = {"sector_name": sector_name, **update_values}
                self.db.execute(text(sql), params)
                count += 1

            except Exception as e:
                logger.warning(f"Error processing sector valuation row: {e}")
                continue

        return count

    def _process_world_market_analysis(self, df: pd.DataFrame, config: Dict) -> int:
        """Insert/update data into world_market_analysis table."""
        entity_col = config["entity_col"]
        mapping = config["mapping"]

        count = 0
        for _, row in df.iterrows():
            try:
                country = str(row[entity_col]).strip()
                if not country or country == "nan":
                    continue

                update_values = {}
                for excel_col, db_col in mapping.items():
                    value = row.get(excel_col)
                    if pd.notna(value):
                        update_values[db_col] = float(value)

                if not update_values:
                    continue

                # Use PostgreSQL ON CONFLICT for upsert
                columns = ["country"] + list(update_values.keys())
                values_placeholders = ", ".join([f":{c}" for c in columns])
                update_clause = ", ".join(
                    [f"{k} = EXCLUDED.{k}" for k in update_values.keys()]
                )

                sql = f"""
                    INSERT INTO world_market_analysis ({", ".join(columns)}) 
                    VALUES ({values_placeholders})
                    ON CONFLICT (country) 
                    DO UPDATE SET {update_clause}
                """

                params = {"country": country, **update_values}
                self.db.execute(text(sql), params)
                count += 1

            except Exception as e:
                logger.warning(f"Error processing world market analysis row: {e}")
                continue

        return count

    def _process_macro_indicators(self, df: pd.DataFrame, config: Dict) -> int:
        """Insert/update data into macro_indicators table."""
        date_col = config["date_col"]
        mapping = config["mapping"]

        count = 0
        for _, row in df.iterrows():
            try:
                month_value = row.get(date_col)
                if pd.isna(month_value):
                    continue

                # Chuyển đổi thành định dạng string như "thg 1-23"
                # Hoặc nếu là date, format lại
                if isinstance(month_value, datetime):
                    month_label = month_value.strftime("%b-%y")  # e.g., "Jan-23"
                else:
                    month_label = str(month_value).strip()

                update_values = {}
                for excel_col, db_col in mapping.items():
                    value = row.get(excel_col)
                    if pd.notna(value):
                        update_values[db_col] = float(value)

                if not update_values:
                    continue

                # Use PostgreSQL ON CONFLICT for upsert
                columns = ["month_label"] + list(update_values.keys())
                values_placeholders = ", ".join([f":{c}" for c in columns])
                update_clause = ", ".join(
                    [f"{k} = EXCLUDED.{k}" for k in update_values.keys()]
                )

                sql = f"""
                    INSERT INTO macro_indicators ({", ".join(columns)}) 
                    VALUES ({values_placeholders})
                    ON CONFLICT (month_label) 
                    DO UPDATE SET {update_clause}
                """

                params = {"month_label": month_label, **update_values}
                self.db.execute(text(sql), params)
                count += 1

            except Exception as e:
                logger.warning(f"Error processing macro indicators row: {e}")
                continue

        return count
