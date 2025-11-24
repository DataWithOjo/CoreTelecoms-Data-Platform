import os
import sys
import boto3
import polars as pl
import argparse
import logging
from datetime import datetime
from typing import Dict, Any

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

def get_aws_credentials(prefix: str = "") -> Dict[str, str]:
    """
    Retrieves AWS credentials from environment variables.
    """
    try:
        p = f"{prefix}_" if prefix else ""
        
        return {
            "aws_access_key_id": os.environ[f"{p}AWS_ACCESS_KEY_ID"],
            "aws_secret_access_key": os.environ[f"{p}AWS_SECRET_ACCESS_KEY"],
            "aws_region": os.environ.get(f"{p}AWS_DEFAULT_REGION", "eu-north-1")
        }
    except KeyError as e:
        logger.error(f"Missing required environment variable: {e}")
        sys.exit(1)

def extract_s3_to_s3(execution_date_str: str, source_bucket: str, source_prefix: str, target_bucket: str, file_type: str):
    logger.info(f"Starting S3 Extraction for date: {execution_date_str} | Type: {file_type}")

    source_creds = get_aws_credentials("SOURCE")
    
    target_creds = get_aws_credentials("")

    try:
        source_s3_client = boto3.client(
            's3',
            aws_access_key_id=source_creds["aws_access_key_id"],
            aws_secret_access_key=source_creds["aws_secret_access_key"],
            region_name=source_creds["aws_region"]
        )
        
        target_s3_client = boto3.client(
            's3',
            aws_access_key_id=target_creds["aws_access_key_id"],
            aws_secret_access_key=target_creds["aws_secret_access_key"],
            region_name=target_creds["aws_region"]
        )
    except Exception as e:
        logger.error(f"Failed to initialize S3 clients: {e}")
        sys.exit(1)

    if execution_date_str == "STATIC":
        source_key = "customers_dataset.csv"
        target_key = "raw/customers/customers_dataset.parquet"
    elif source_prefix == "call_logs":
        source_key = f"call_logs_day_{execution_date_str}.csv"
        target_key = f"raw/call_logs/{execution_date_str}/data.parquet"
    elif source_prefix == "media_complaint":
        source_key = f"media_complaint_day_{execution_date_str}.json"
        target_key = f"raw/social_media/{execution_date_str}/data.parquet"
    else:
        logger.error(f"Unknown source prefix: {source_prefix}")
        sys.exit(1)

    logger.info(f"Target Source: s3://{source_bucket}/{source_key}")


    try:
        try:
            source_s3_client.head_object(Bucket=source_bucket, Key=source_key)
        except Exception:
            logger.warning(f"File {source_key} not found in source bucket. Skipping this run.")
            sys.exit(0)

        logger.info("Reading data into Polars...")
        
        if file_type == "csv":
            df = pl.read_csv(
                f"s3://{source_bucket}/{source_key}",
                storage_options=source_creds,
                infer_schema_length=0
            )
            
        elif file_type == "json":
            logger.info("Detected JSON input. Parsing as Column-Oriented...")
            df = pl.read_json(
                f"s3://{source_bucket}/{source_key}",
                storage_options=source_creds
            )
            
        current_time = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
        df = df.with_columns(pl.lit(current_time).alias("load_time"))

        logger.info(f"Data Loaded: {df.height} rows processed.")

        local_path = f"/tmp/{source_key.replace('/', '_')}.parquet"
        df.write_parquet(local_path)
        
        logger.info(f"Uploading to s3://{target_bucket}/{target_key}")
        target_s3_client.upload_file(local_path, target_bucket, target_key)
        
        os.remove(local_path)
        logger.info("Extraction & Upload Completed Successfully.")

    except Exception as e:
        logger.error(f"Critical Extraction Failure: {e}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--execution_date", required=True)
    parser.add_argument("--source_bucket", required=True)
    parser.add_argument("--source_prefix", required=True)
    parser.add_argument("--target_bucket", required=True)
    parser.add_argument("--file_type", required=True)
    args = parser.parse_args()
    
    extract_s3_to_s3(
        args.execution_date, 
        args.source_bucket, 
        args.source_prefix, 
        args.target_bucket, 
        args.file_type
    )