from airflow.sdk import Asset

S3_RAW_DATA_READY = Asset("s3://coretelecoms-raw-zone-oluwakayode-dev/ready")
SNOWFLAKE_RAW_READY = Asset("snowflake://core-telecoms-dw/raw_ready")