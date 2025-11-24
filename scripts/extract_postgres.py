import os
import sys
import boto3
import polars as pl
import argparse
import logging
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

def extract_and_upload(execution_date_str, target_bucket):
    logger.info(f"Starting Postgres extraction for date: {execution_date_str}")
    
    SCHEMA_NAME = "customer_complaints"
    
    try:
        date_obj = datetime.strptime(execution_date_str, "%Y-%m-%d")
        table_suffix = date_obj.strftime("%Y_%m_%d")
        table_name = f"Web_form_request_{table_suffix}"
        full_table_name = f"{SCHEMA_NAME}.{table_name}"
        logger.info(f"Targeting Table: {full_table_name}")
    except ValueError as e:
        logger.error(f"Date format error: {e}")
        sys.exit(1)

    try:
        db_user = os.getenv("POSTGRES_USER")
        db_pass = os.getenv("POSTGRES_PASSWORD")
        db_host = os.getenv("POSTGRES_HOST")
        db_port = os.getenv("POSTGRES_PORT")
        db_name = os.getenv("POSTGRES_DB")
        
        if not all([db_user, db_pass, db_host, db_name]):
             raise ValueError("Missing one or more Postgres environment variables.")

        uri = f"postgresql://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}"
    except Exception as e:
        logger.error(f"Config Error: {e}")
        sys.exit(1)

    try:
        query = f"SELECT * FROM {full_table_name}"
        logger.info(f"Executing Query: {query}")
        
        df = pl.read_database_uri(query, uri)
        
        current_time = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
        df = df.with_columns(pl.lit(current_time).alias("load_time"))

        logger.info(f"Extracted {df.height} rows.")
        
        local_file = f"web_complaints_{table_suffix}.parquet"
        local_path = f"/tmp/{local_file}"
        df.write_parquet(local_path)
        
        s3_key = f"raw/postgres/{execution_date_str}/{local_file}"
        logger.info(f"Uploading to s3://{target_bucket}/{s3_key}...")
        
        s3 = boto3.client('s3')
        s3.upload_file(local_path, target_bucket, s3_key)
        
        os.remove(local_path)
        logger.info("Success!")

    except Exception as e:
        if "relation" in str(e) and "does not exist" in str(e):
            logger.warning(f"Table {full_table_name} not found. Skipping this date.")
            sys.exit(0)
        else:
            logger.error(f"Critical Extraction Failure: {e}", exc_info=True)
            sys.exit(1)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--execution_date", required=True)
    parser.add_argument("--target_bucket", required=True)
    args = parser.parse_args()
    
    extract_and_upload(args.execution_date, args.target_bucket)