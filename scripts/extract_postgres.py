import os
import sys
import boto3
import polars as pl
import argparse
import logging
import re
from datetime import datetime
from typing import Dict, Any
from sqlalchemy import create_engine

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger("postgres_extractor")

class ConfigError(Exception):
    """Raised when configuration or environment variables are missing."""
    pass

class ExtractionError(Exception):
    """Raised when the extraction process fails."""
    pass

def get_db_uri() -> str:
    """Constructs the Postgres connection URI from environment variables."""
    try:
        user = os.environ["POSTGRES_USER"]
        password = os.environ["POSTGRES_PASSWORD"]
        host = os.environ["POSTGRES_HOST"]
        port = os.environ.get("POSTGRES_PORT", "5432")
        db_name = os.environ["POSTGRES_DB"]
        
        return f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db_name}"
    except KeyError as e:
        raise ConfigError(f"Missing required environment variable: {e}")

def get_s3_client() -> Any:
    """Initialize Boto3 S3 client."""
    try:
        return boto3.client('s3')
    except Exception as e:
        raise ConfigError(f"Failed to initialize S3 client: {e}")

def clean_column_name(col_name: str) -> str:
    """
    Standardizes column names to snake_case.
    
    Args:
        col_name: The original column name.
        
    Returns:
        str: Cleaned snake_case column name.
    """
    raw = str(col_name).lower()
    
    if raw.startswith('unnamed') or raw == 'column1':
        return 'source_row_id'
    
    clean = re.sub(r'[^a-z0-9]+', '_', raw)
    
    return clean.strip('_')

def extract_and_upload(execution_date_str: str, target_bucket: str) -> None:
    """
    Main ETL function: Extract from DB to S3 with column sanitization.
    """
    logger.info(f"Starting Postgres extraction for date: {execution_date_str}")
    
    SCHEMA_NAME = "customer_complaints"
    local_path = None

    try:
        uri = get_db_uri()
        s3 = get_s3_client()

        try:
            date_obj = datetime.strptime(execution_date_str, "%Y-%m-%d")
            table_suffix = date_obj.strftime("%Y_%m_%d")
            table_name = f"Web_form_request_{table_suffix}"
            full_table_name = f"{SCHEMA_NAME}.{table_name}"
            logger.info(f"Targeting Table: {full_table_name}")
        except ValueError as e:
            raise ConfigError(f"Invalid date format. Expected YYYY-MM-DD. Error: {e}")

        query = f"SELECT * FROM {full_table_name}"
        logger.info(f"Executing Query: {query}")
        
        try:
            engine = create_engine(uri, pool_pre_ping=True)
            
            with engine.connect() as connection:
                df = pl.read_database(
                    query=query, 
                    connection=connection
                )
                
        except Exception as e:
            error_str = str(e).lower()
            if "relation" in error_str and "does not exist" in error_str:
                logger.warning(f"SKIPPING: Table {full_table_name} not found. Data not ready.")
                sys.exit(99)
            else:
                raise e

        cleaning_map = {
            col: clean_column_name(col)
            for col in df.columns
        }
        df = df.rename(cleaning_map)
        logger.info(f"Sanitized Columns: {df.columns}")

        current_time = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
        df = df.with_columns(pl.lit(current_time).alias("load_time"))
        
        logger.info(f"Extracted {df.height} rows.")
        
        local_file = f"web_complaints_{table_suffix}.parquet"
        local_path = f"/tmp/{local_file}"
        df.write_parquet(local_path)
        
        s3_key = f"raw/postgres/{execution_date_str}/{local_file}"
        logger.info(f"Uploading to s3://{target_bucket}/{s3_key}...")
        
        s3.upload_file(local_path, target_bucket, s3_key)
        logger.info("Upload Success!")

    except (ConfigError, ExtractionError) as e:
        logger.error(f"Operational Error: {e}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Unexpected System Error: {e}", exc_info=True)
        sys.exit(1)
    finally:
        if local_path and os.path.exists(local_path):
            logger.info(f"Cleaning up local file: {local_path}")
            os.remove(local_path)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Postgres to S3 Extraction Script")
    parser.add_argument("--execution_date", required=True, help="Logical date (YYYY-MM-DD)")
    parser.add_argument("--target_bucket", required=True, help="S3 Bucket Name")
    args = parser.parse_args()
    
    extract_and_upload(args.execution_date, args.target_bucket)