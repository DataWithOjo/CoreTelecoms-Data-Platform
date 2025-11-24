import os
import sys
import boto3
import polars as pl
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime
import argparse
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

def extract_gsheets_to_s3(target_bucket: str):
    logger.info("Starting Google Sheets Extraction (Agents)")

    KEY_PATH = "/opt/airflow/config/google_credentials.json"
    
    if not os.path.exists(KEY_PATH):
        logger.error(f"Google Credentials file not found at: {KEY_PATH}")
        sys.exit(1)

    try:
        scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        creds = ServiceAccountCredentials.from_json_keyfile_name(KEY_PATH, scope)
        client = gspread.authorize(creds)
        logger.info("Authenticated with Google.")
    except Exception as e:
        logger.error(f"Auth Failed: {e}")
        sys.exit(1)

    SHEET_NAME = os.getenv("GOOGLE_SHEETS_AGENTS_FILE", "CORETELECOMMS AGENTS")
    
    try:
        spreadsheet = client.open(SHEET_NAME)
        
        sheet = spreadsheet.sheet1
        logger.info(f"Accessing Tab: {sheet.title}")
        
        data = sheet.get_all_records()
        
        if not data:
            logger.warning("Sheet is empty.")
            sys.exit(0)
            
        logger.info(f"Retrieved {len(data)} rows.")

        df = pl.DataFrame(data)
        
        logger.info(f"Original Columns: {df.columns}")
        
        new_columns = {col: col.lower() for col in df.columns}
        df = df.rename(new_columns)
        
        if "id" in df.columns:
            df = df.with_columns(pl.col("id").cast(pl.Utf8))

        logger.info(f"Cleaned Columns: {df.columns}")

    except gspread.SpreadsheetNotFound:
        logger.error(f"Spreadsheet '{SHEET_NAME}' not found. Please SHARE the sheet with the service account email.")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Failed to read sheet: {e}")
        sys.exit(1)

    try:
        current_time = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
        df = df.with_columns(pl.lit(current_time).alias("load_time"))

        local_path = "/tmp/agents.parquet"
        df.write_parquet(local_path)
        
        s3_key = "raw/agents/agents_list.parquet"
        
        target_opts = {
            "aws_access_key_id": os.getenv("AWS_ACCESS_KEY_ID"),
            "aws_secret_access_key": os.getenv("AWS_SECRET_ACCESS_KEY"),
            "aws_region": os.getenv("AWS_DEFAULT_REGION", "eu-north-1")
        }
        
        s3 = boto3.client(
            's3',
            aws_access_key_id=target_opts["aws_access_key_id"],
            aws_secret_access_key=target_opts["aws_secret_access_key"],
            region_name=target_opts["aws_region"]
        )
        
        logger.info(f"Uploading to s3://{target_bucket}/{s3_key}")
        s3.upload_file(local_path, target_bucket, s3_key)
        
        os.remove(local_path)
        logger.info("Google Sheets Extraction Success!")

    except Exception as e:
        logger.error(f"Upload Failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--target_bucket", required=True)
    args = parser.parse_args()
    
    extract_gsheets_to_s3(args.target_bucket)