import os
import sys
import boto3
import polars as pl
import gspread
import re
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime
import argparse
import logging
from typing import Dict, Any, Optional

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger("gsheets_extractor")

KEY_PATH = "/opt/airflow/config/google_credentials.json"
DEFAULT_SHEET_NAME = "CORETELECOMMS AGENTS"
SCOPE = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']

class ConfigError(Exception):
    """Raised when configuration or environment variables are missing."""
    pass

class ExtractionError(Exception):
    """Raised when the extraction process fails."""
    pass

def get_google_client(key_path: str, scope: list) -> gspread.Client:
    """
    Authenticates with Google APIs and returns a gspread client.
    """
    if not os.path.exists(key_path):
        raise ConfigError(f"Google Credentials file not found at: {key_path}")

    try:
        creds = ServiceAccountCredentials.from_json_keyfile_name(key_path, scope)
        client = gspread.authorize(creds)
        logger.info("Authenticated with Google.")
        return client
    except Exception as e:
        raise ExtractionError(f"Authentication failed: {e}")

def get_sheet_data(client: gspread.Client, sheet_name: str) -> pl.DataFrame:
    """
    Opens the Google Sheet and reads data into a Polars DataFrame.
    """
    try:
        spreadsheet = client.open(sheet_name)
        logger.info(f"Found Spreadsheet: {sheet_name}")
        
        sheet = spreadsheet.sheet1
        logger.info(f"Accessing Tab: {sheet.title}")
        
        data = sheet.get_all_records()
        
        if not data:
            logger.warning("Sheet is empty.")
            sys.exit(0)
            
        logger.info(f"Retrieved {len(data)} rows.")
        return pl.DataFrame(data)

    except gspread.SpreadsheetNotFound:
        raise ExtractionError(f"Spreadsheet '{sheet_name}' not found. Please SHARE the sheet with the service account email.")
    except Exception as e:
        raise ExtractionError(f"Failed to read sheet '{sheet_name}': {e}")

def clean_column_name(col_name: str) -> str:
    """
    Standardizes column names to snake_case.
    
    Args:
        col_name: The original column name.
        
    Returns:
        str: Cleaned snake_case column name.
    """
    raw = str(col_name).lower()
    
    if raw.startswith('unnamed') or raw == 'column1' or raw == '':
        return 'source_row_id'
    
    clean = re.sub(r'[^a-z0-9]+', '_', raw)
    
    return clean.strip('_')

def process_dataframe(df: pl.DataFrame) -> pl.DataFrame:
    """
    Cleans column names and adds audit columns.
    """
    try:
        logger.info(f"Original Columns: {df.columns}")
        
        cleaning_map = {
            col: clean_column_name(col)
            for col in df.columns
        }
        df = df.rename(cleaning_map)
        
        if "id" in df.columns:
            df = df.with_columns(pl.col("id").cast(pl.Utf8))
            
        logger.info(f"Cleaned Columns: {df.columns}")

        current_time = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
        df = df.with_columns(pl.lit(current_time).alias("load_time"))
        
        return df
    except Exception as e:
        raise ExtractionError(f"Data processing failed: {e}")

def upload_to_s3(local_path: str, target_bucket: str, s3_key: str) -> None:
    """
    Uploads a local file to S3 using environment variables for credentials.
    """
    try:
        target_opts = {
            "aws_access_key_id": os.getenv("AWS_ACCESS_KEY_ID"),
            "aws_secret_access_key": os.getenv("AWS_SECRET_ACCESS_KEY"),
            "region_name": os.getenv("AWS_DEFAULT_REGION", "eu-north-1")
        }
        
        if not all(target_opts.values()):
             raise ConfigError("Missing AWS credentials in environment variables.")

        s3 = boto3.client('s3', **target_opts)
        
        logger.info(f"Uploading to s3://{target_bucket}/{s3_key}")
        s3.upload_file(local_path, target_bucket, s3_key)
        logger.info("Google Sheets Extraction Success!")
        
    except Exception as e:
        raise ExtractionError(f"S3 Upload Failed: {e}")

def extract_gsheets_to_s3(target_bucket: str) -> None:
    """
    Orchestrates the extraction from Google Sheets to S3.
    """
    logger.info("Starting Google Sheets Extraction (Agents)")
    
    sheet_name = os.getenv("GOOGLE_SHEETS_AGENTS_FILE", DEFAULT_SHEET_NAME)
    local_path = "/tmp/agents.parquet"
    s3_key = "raw/agents/agents_list.parquet"

    try:
        client = get_google_client(KEY_PATH, SCOPE)
        df = get_sheet_data(client, sheet_name)
        
        df = process_dataframe(df)
        
        df.write_parquet(local_path)
        
        upload_to_s3(local_path, target_bucket, s3_key)

    except (ConfigError, ExtractionError) as e:
        logger.error(f"Operation Failed: {e}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Unexpected Error: {e}", exc_info=True)
        sys.exit(1)
    finally:
        if os.path.exists(local_path):
            os.remove(local_path)
            logger.info("Cleaned up local file.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Google Sheets to S3 Extractor")
    parser.add_argument("--target_bucket", required=True, help="S3 Bucket Name")
    
    try:
        args = parser.parse_args()
        extract_gsheets_to_s3(args.target_bucket)
    except Exception as e:
        logger.critical(f"Unhandled Script Exception: {e}")
        sys.exit(1)