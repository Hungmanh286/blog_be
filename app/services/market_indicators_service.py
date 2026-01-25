"""
Market Indicators Service
Handles Excel file upload and merges all sheets by date into market_indicators table.
"""

import io
import logging
from typing import Dict, Any
from datetime import datetime, date

import pandas as pd
from sqlalchemy.orm import Session
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

logger = logging.getLogger(__name__)

# Minimum allowed date - filter out any dates before this
MIN_DATE = datetime(2000, 1, 1)


SHEET_COLUMN_MAPPING = {
    'Vol': {
        'date_col': 'Date',
        'columns': {
            'Volatility Index': 'volatility_index'
        }
    },
    'Bre': {
        'date_col': 'Date',
        'columns': {
            'Breadth Index': 'breadth_index'
        }
    },
    'Map': {
        'date_col': 'Date',
        'columns': {
            'Market price': 'market_price'
        }
    },
    'Div': {
        'date_col': 'Date',
        'columns': {
            '% Dividen 12M': 'dividend_12m_pct'
        }
    },
    'Eps': {
        'date_col': 'Date',
        'columns': {
            'Vnindex': 'eps_vnindex',
            'Nonbank': 'eps_nonbank'
        }
    },
    '%St': {
        'date_col': 'Unnamed: 0',
        'columns': {
            '% PE < 10': 'st_pe_under_10_pct',
            '% PB < 1': 'st_pb_under_1_pct'
        }
    },
    'PB': {
        'date_col': 'Date',
        'columns': {
            'Vnindex': 'pb_vnindex',
            'Non bank': 'pb_nonbank',
            'Bank': 'pb_bank'
        }
    },
    'Ret': {
        'date_col': 'Unnamed: 0',  # Cột đầu tiên không có tên
        'columns': {
            '40 Years Old': 'ret_40years_old_pct',
            'VNI Adjusted': 'ret_vni_adjusted_pct'
        }
    },
    'Tur': {
        'date_col': 'Date',
        'columns': {
            'Turnover ratio': 'turnover_ratio'
        }
    },
    'Der': {
        'date_col': 'Date',
        'columns': {
            'Derivaties ratio': 'derivatives_ratio'
        }
    },
    'Ins': {
        'date_col': 'Date',
        'columns': {
            '% Insider Transaction 3M': 'insider_transaction_3m_pct'
        }
    },
    'Tra': {
        'date_col': 'Date',
        'columns': {
            'Probability': 'probability'
        }
    },
    'Avg': {
        'date_col': 'Date',
        'columns': {
            'Avg 50D Orders': 'avg_50d_orders'
        }
    },
    'Mat': {
        'date_col': 'Date',
        'columns': {
            'Matching Rate (%)': 'matching_rate_pct'
        }
    },
    'Cor': {
        'date_col': 'Date',
        'columns': {
            'SPX': 'cor_spx',
            'VN1Y': 'cor_vn1y',
            'USD': 'cor_usd'
        }
    },
    'Hea': {
        'date_col': 'Date',
        'columns': {
            'Consumption': 'hea_consumption',
            'Production': 'hea_production',
            'Labor': 'hea_labor'
        }
    }
}


class MarketIndicatorsService:
    """Service to process and import market indicators from Excel files."""
    
    def __init__(self, db: Session):
        self.db = db
    
    def process_excel_file(self, file_content: bytes, filename: str) -> Dict[str, Any]:
        """
        Process Excel file: merge all sheets by date and insert into market_indicators.
        
        Args:
            file_content: Binary content of Excel file
            filename: Name of the uploaded file
            
        Returns:
            Dictionary containing processing results
        """
        try:
            # Read Excel file
            excel_file = pd.ExcelFile(io.BytesIO(file_content))
            logger.info(f"Processing Excel file: {filename} with sheets: {excel_file.sheet_names}")
            
            # Merge all sheets by date
            merged_df = self._merge_all_sheets(excel_file)
            
            if merged_df.empty:
                return {
                    'filename': filename,
                    'status': 'error',
                    'message': 'No data to process',
                    'records_inserted': 0
                }
            
            # Insert into database
            records_inserted = self._insert_to_db(merged_df)
            
            return {
                'filename': filename,
                'status': 'success',
                'message': f'Successfully processed {records_inserted} records',
                'records_inserted': records_inserted,
                'total_dates': len(merged_df),
                'date_range': {
                    'from': str(merged_df['report_date'].min()),
                    'to': str(merged_df['report_date'].max())
                }
            }
            
        except Exception as e:
            logger.error(f"Error processing Excel file: {e}", exc_info=True)
            self.db.rollback()
            return {
                'filename': filename,
                'status': 'error',
                'message': str(e),
                'records_inserted': 0
            }
    
    def _merge_all_sheets(self, excel_file: pd.ExcelFile) -> pd.DataFrame:
        """
        Merge all sheets by date column.
        
        Strategy:
        1. First pass: Find MIN and MAX dates across ALL sheets
        2. Create CONTINUOUS date range from min to max (including all days)
        3. Left join each sheet to date range
        4. Result: Complete date range preserved, missing values = NULL
        
        Args:
            excel_file: Pandas ExcelFile object
            
        Returns:
            Merged DataFrame with all indicators by date
        """
        min_date = None
        max_date = None
        sheet_data = {} 
        
        for sheet_name in excel_file.sheet_names:
            if sheet_name not in SHEET_COLUMN_MAPPING:
                logger.warning(f"Sheet '{sheet_name}' not in mapping, skipping")
                continue
            
            try:
                config = SHEET_COLUMN_MAPPING[sheet_name]
                df = pd.read_excel(excel_file, sheet_name)
                
                # Get date column
                date_col = config['date_col']
                if date_col not in df.columns:
                    logger.warning(f"Sheet '{sheet_name}' missing date column '{date_col}', skipping")
                    continue
                
                # Rename date column to 'report_date'
                df = df.rename(columns={date_col: 'report_date'})
                
                # Convert to datetime
                df['report_date'] = pd.to_datetime(df['report_date'], errors='coerce')
                
                # Remove rows with invalid dates
                df = df.dropna(subset=['report_date'])
                
                # Filter out dates before MIN_DATE (2000-01-01)
                original_count = len(df)
                df = df[df['report_date'] >= MIN_DATE]
                filtered_count = original_count - len(df)
                
                if filtered_count > 0:
                    logger.warning(f"Sheet '{sheet_name}': Filtered out {filtered_count} rows with dates before {MIN_DATE.date()}")

                
                if len(df) == 0:
                    logger.warning(f"Sheet '{sheet_name}' has no valid dates, skipping")
                    continue
                
                # Track min/max dates
                sheet_min = df['report_date'].min()
                sheet_max = df['report_date'].max()
                
                if min_date is None or sheet_min < min_date:
                    min_date = sheet_min
                if max_date is None or sheet_max > max_date:
                    max_date = sheet_max
                
                # Select and rename indicator columns
                columns_to_keep = ['report_date']
                rename_mapping = {}
                
                for excel_col, db_col in config['columns'].items():
                    if excel_col in df.columns:
                        rename_mapping[excel_col] = db_col
                        columns_to_keep.append(excel_col)
                
                # Keep only relevant columns and rename
                df = df[columns_to_keep].rename(columns=rename_mapping)
                
                # Store processed sheet for second pass
                sheet_data[sheet_name] = df
                
                logger.info(f"Sheet '{sheet_name}': {len(df)} rows, date range: {sheet_min.date()} to {sheet_max.date()}")
                
            except Exception as e:
                logger.error(f"Error processing sheet '{sheet_name}': {e}")
                continue
        
        if min_date is None or max_date is None:
            logger.warning("No valid dates found in any sheet")
            return pd.DataFrame()
        
        # STEP 2: Create CONTINUOUS date range from min to max
        date_range = pd.date_range(start=min_date, end=max_date, freq='D')
        merged_df = pd.DataFrame({'report_date': date_range})
        
        logger.info(f"Created continuous date range with {len(date_range)} dates")
        logger.info(f"Date range: {min_date.date()} to {max_date.date()}")
        
        # STEP 3: Left join each sheet to date range
        for sheet_name, df in sheet_data.items():
            merged_df = pd.merge(
                merged_df,
                df,
                on='report_date',
                how='left'  # LEFT JOIN: Keep all dates from range, fill missing with NULL
            )
            logger.info(f"Merged sheet '{sheet_name}' to date range")
        
        # Convert report_date to date only (remove time)
        merged_df['report_date'] = merged_df['report_date'].dt.date
        
        # Sort by date (already sorted, but just to be safe)
        merged_df = merged_df.sort_values('report_date')
        
        logger.info(f"Final merged dataframe shape: {merged_df.shape}")
        logger.info(f"Columns: {list(merged_df.columns)}")
        
        return merged_df
    
    def _insert_to_db(self, df: pd.DataFrame) -> int:
        """
        Insert merged dataframe into market_indicators table using upsert.
        
        Args:
            df: Merged DataFrame with all indicators
            
        Returns:
            Number of records inserted/updated
        """
        count = 0
        
        for _, row in df.iterrows():
            try:
                # Prepare data
                report_date = row['report_date']
                
                # Get all non-null values except report_date
                update_values = {}
                for col in df.columns:
                    if col != 'report_date':
                        value = row[col]
                        if pd.notna(value):
                            update_values[col] = float(value) if isinstance(value, (int, float)) else value
                
                # Build SQL for upsert
                # Insert row even if all values are NULL (to preserve date range)
                if update_values:
                    # Has data - do upsert with data
                    columns = ['report_date'] + list(update_values.keys())
                    values_placeholders = ', '.join([f':{c}' for c in columns])
                    update_clause = ', '.join([f'{k} = EXCLUDED.{k}' for k in update_values.keys()])
                    
                    sql = f"""
                        INSERT INTO market_indicators ({', '.join(columns)}) 
                        VALUES ({values_placeholders})
                        ON CONFLICT (report_date) 
                        DO UPDATE SET {update_clause}
                    """
                    
                    params = {'report_date': report_date, **update_values}
                else:
                    # No data - insert just the date (other columns will be NULL)
                    sql = """
                        INSERT INTO market_indicators (report_date) 
                        VALUES (:report_date)
                        ON CONFLICT (report_date) DO NOTHING
                    """
                    params = {'report_date': report_date}
                
                self.db.execute(text(sql), params)
                count += 1
                
            except Exception as e:
                logger.warning(f"Error inserting row for date {row.get('report_date')}: {e}")
                continue
        
        # Commit all changes
        self.db.commit()
        logger.info(f"Inserted/updated {count} records to market_indicators")
        
        return count
