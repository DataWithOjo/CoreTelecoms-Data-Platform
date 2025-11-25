import os
import sys
import boto3
import polars as pl
import argparse
import logging
import json 
from datetime import datetime
from typing import Dict, Any

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger("s3_extractor")

class ExtractionError(Exception):
    pass

def get_aws_credentials(prefix: str = "") -> Dict[str, str]:
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
    try:
        return boto3.client('s3', **creds)
    except Exception as e:
        raise ExtractionError(f"Failed to initialize S3 client: {e}")

def construct_paths(execution_date: str, prefix: str) -> Dict[str, str]:
    """
    Maps Logical Prefix (from DAG) to Physical S3 Path.
    """

    if execution_date == "STATIC" or prefix == "static":
        return {
            "source_key": "customers/customers_dataset.csv",
            "target_key": "raw/customers/customers_dataset.parquet"
        }
    
    if prefix == "call_logs":
        return {
            "source_key": f"call logs/call_logs_day_{execution_date}.csv",
            "target_key": f"raw/call_logs/{execution_date}/call_logs_{execution_date}.parquet"
        }
    
    if prefix == "media_complaint":
        return {
            "source_key": f"social_medias/media_complaint_day_{execution_date}.json",
            "target_key": f"raw/social_media/{execution_date}/social_media_{execution_date}.parquet"
        }
    
    raise ExtractionError(f"Unknown source prefix: {prefix}")

def process_file(local_path: str, file_type: str) -> pl.DataFrame:
    try:
        df = None
        if file_type == "csv":
            df = pl.read_csv(local_path, infer_schema_length=0)
        
        elif file_type == "json":
            logger.info("Parsing JSON (Standard Library)...")
            with open(local_path, 'r') as f:
                data = json.load(f)
            logger.info("Converting to Polars DataFrame...")
            df = pl.DataFrame(data)
            
        else:
            raise ExtractionError(f"Unsupported file type: {file_type}")

        new_columns = {
            col: col.strip().lower().replace(" ", "_") 
            for col in df.columns
        }
        df = df.rename(new_columns)
        
        return df
        
    except Exception as e:
        raise ExtractionError(f"Failed to parse file {local_path}: {e}")

def extract_s3_to_s3(
    execution_date_str: str, 
    source_bucket: str, 
    source_prefix: str, 
    target_bucket: str, 
    file_type: str
) -> None:
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
            logger.info(f"⬇Downloading to {temp_input}...")
            source_s3.download_file(source_bucket, source_key, temp_input)
        except Exception as e:
            if "404" in str(e) or "Not Found" in str(e):
                logger.warning(f"SKIPPING: File {source_key} not found in S3. Data likely not ready.")
                sys.exit(99)
            else:
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

    except (ExtractionError, Exception) as e:
        logger.error(f"Operation Failed: {e}")
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