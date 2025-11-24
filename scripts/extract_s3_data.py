import os
import sys
import boto3
import polars as pl
import argparse
import logging
from datetime import datetime
from typing import Dict, Any, Optional

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger("s3_extractor")

class ExtractionError(Exception):
    """Custom exception for extraction failures."""
    pass

def get_aws_credentials(prefix: str = "") -> Dict[str, str]:
    """
    Retrieves AWS credentials from environment variables safely.
    
    Args:
        prefix (str): Prefix for env vars (e.g., 'SOURCE' for 'SOURCE_AWS_ACCESS_KEY_ID').
    
    Returns:
        Dict[str, str]: Dictionary compatible with boto3 client.
    """
    p = f"{prefix}_" if prefix else ""
    try:
        return {
            "aws_access_key_id": os.environ[f"{p}AWS_ACCESS_KEY_ID"],
            "aws_secret_access_key": os.environ[f"{p}AWS_SECRET_ACCESS_KEY"],
            "region_name": os.environ.get(f"{p}AWS_DEFAULT_REGION", "eu-north-1")
        }
    except KeyError as e:
        raise ExtractionError(f"Missing environment variable: {e}")

def get_s3_client(creds: Dict[str, str]) -> Any:
    """Initialize Boto3 S3 client with error handling."""
    try:
        return boto3.client('s3', **creds)
    except Exception as e:
        raise ExtractionError(f"Failed to initialize S3 client: {e}")

def construct_paths(execution_date: str, prefix: str) -> Dict[str, str]:
    """
    Determines source and target paths based on the dataset type and date.
    
    Args:
        execution_date (str): Logical date (YYYY-MM-DD).
        prefix (str): Dataset identifier ('call_logs', 'media_complaint', etc.).
        
    Returns:
        Dict[str, str]: 'source_key' and 'target_key'.
    """
    if execution_date == "STATIC":
        return {
            "source_key": "customers/customers_dataset.csv",
            "target_key": "raw/customers/customers_dataset.parquet"
        }
    
    if prefix == "call_logs":
        return {
            "source_key": f"call logs/call_logs_day_{execution_date}.csv",
            "target_key": f"raw/call_logs/{execution_date}/data.parquet"
        }
    
    if prefix == "media_complaint":
        return {
            "source_key": f"social_medias/media_complaint_day_{execution_date}.json",
            "target_key": f"raw/social_media/{execution_date}/data.parquet"
        }
    
    raise ExtractionError(f"Unknown source prefix: {prefix}")

def process_file(local_path: str, file_type: str) -> pl.DataFrame:
    """
    Reads local file into Polars DataFrame based on type.
    """
    try:
        if file_type == "csv":
            return pl.read_csv(local_path, infer_schema_length=0)
        
        if file_type == "json":
            logger.info("Parsing JSON (handling column-oriented format)...")
            return pl.read_json(local_path)
            
        raise ExtractionError(f"Unsupported file type: {file_type}")
        
    except Exception as e:
        raise ExtractionError(f"Failed to parse file {local_path}: {e}")

def extract_s3_to_s3(
    execution_date_str: str, 
    source_bucket: str, 
    source_prefix: str, 
    target_bucket: str, 
    file_type: str
) -> None:
    """
    Main orchestration function for S3->S3 extraction.
    """
    logger.info(f"Starting S3 Extraction for {execution_date_str} | Type: {file_type}")

    try:
        source_s3 = get_s3_client(get_aws_credentials("SOURCE"))
        target_s3 = get_s3_client(get_aws_credentials(""))

        paths = construct_paths(execution_date_str, source_prefix)
        source_key = paths["source_key"]
        target_key = paths["target_key"]
        
        logger.info(f"Target Source: s3://{source_bucket}/{source_key}")

        safe_name = source_key.replace('/', '_').replace(' ', '_')
        temp_input = f"/tmp/input_{safe_name}"
        
        try:
            logger.info(f"Downloading to {temp_input}...")
            source_s3.download_file(source_bucket, source_key, temp_input)
        except Exception as e:
            if "404" in str(e) or "Not Found" in str(e):
                logger.error(f"FILE MISSING: {source_key}")
                sys.exit(1)
            raise ExtractionError(f"Download failed: {e}")

        df = process_file(temp_input, file_type)
        
        current_time = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
        df = df.with_columns(pl.lit(current_time).alias("load_time"))
        
        logger.info(f"Processed {df.height} rows.")

        temp_output = f"/tmp/output_{safe_name}.parquet"
        df.write_parquet(temp_output)
        
        logger.info(f"Uploading to s3://{target_bucket}/{target_key}")
        target_s3.upload_file(temp_output, target_bucket, target_key)

        if os.path.exists(temp_input): os.remove(temp_input)
        if os.path.exists(temp_output): os.remove(temp_output)
        
        logger.info("Extraction Success!")

    except ExtractionError as e:
        logger.error(f"Logical Error: {e}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Unexpected System Error: {e}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="S3 to S3 ETL Script")
    parser.add_argument("--execution_date", required=True, help="YYYY-MM-DD or 'STATIC'")
    parser.add_argument("--source_bucket", required=True, help="Source S3 Bucket Name")
    parser.add_argument("--source_prefix", required=True, help="Dataset prefix (call_logs, media_complaint)")
    parser.add_argument("--target_bucket", required=True, help="Target S3 Bucket Name")
    parser.add_argument("--file_type", required=True, choices=['csv', 'json'], help="Input file format")
    
    args = parser.parse_args()
    
    extract_s3_to_s3(
        args.execution_date, 
        args.source_bucket, 
        args.source_prefix, 
        args.target_bucket, 
        args.file_type
    )