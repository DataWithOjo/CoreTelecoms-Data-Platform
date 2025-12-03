USE ROLE ACCOUNTADMIN;
CREATE DATABASE IF NOT EXISTS CORETELECOMS_DW;
CREATE SCHEMA IF NOT EXISTS CORETELECOMS_DW.RAW;
USE SCHEMA CORETELECOMS_DW.RAW;

CREATE OR REPLACE FILE FORMAT PARQUET_FORMAT
    TYPE = 'PARQUET'
    COMPRESSION = 'SNAPPY'
    BINARY_AS_TEXT = FALSE;

CREATE OR REPLACE STAGE MY_S3_STAGE
    URL = 's3://coretelecoms-raw-zone-oluwakayode-dev/raw/'
    CREDENTIALS = (AWS_KEY_ID='xxxx' AWS_SECRET_KEY='xxxx')
    FILE_FORMAT = PARQUET_FORMAT;

CREATE OR REPLACE TABLE CUSTOMERS (
    customer_id VARCHAR,
    name VARCHAR,
    gender VARCHAR,
    date_of_birth VARCHAR,
    signup_date VARCHAR,
    email VARCHAR,
    address VARCHAR,
    load_time VARCHAR,
    _ingestion_time TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE AGENTS (
    id VARCHAR,
    name VARCHAR,
    experience VARCHAR,
    state VARCHAR,
    load_time VARCHAR,
    _ingestion_time TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE CALL_CENTER_LOGS (
    source_row_id VARCHAR,
    call_id VARCHAR,
    customer_id VARCHAR,
    complaint_catego_ry VARCHAR,
    agent_id VARCHAR,
    call_start_time VARCHAR,
    call_end_time VARCHAR,
    resolutionstatus VARCHAR,
    calllogsgenerationdate VARCHAR,
    load_time VARCHAR,
    _ingestion_time TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE WEB_COMPLAINTS (
    source_row_id VARCHAR,
    request_id VARCHAR,
    customer_id VARCHAR,
    complaint_catego_ry VARCHAR,
    agent_id VARCHAR,
    resolutionstatus VARCHAR,
    request_date VARCHAR,
    resolution_date VARCHAR,
    webformgenerationdate VARCHAR,
    load_time VARCHAR,
    _ingestion_time TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE SOCIAL_MEDIA (
    complaint_id VARCHAR,
    customer_id VARCHAR,
    complaint_catego_ry VARCHAR,
    agent_id VARCHAR,
    resolutionstatus VARCHAR,
    request_date VARCHAR,
    resolution_date VARCHAR,
    media_channel VARCHAR,
    mediacomplaintgenerationdate VARCHAR,
    load_time VARCHAR,
    _ingestion_time TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);