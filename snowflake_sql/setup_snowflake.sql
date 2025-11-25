-- Created File Format for Parquet
CREATE OR REPLACE FILE FORMAT CORETELECOMS_DW.RAW.PARQUET_FORMAT
    TYPE = 'PARQUET'
    COMPRESSION = 'SNAPPY';

-- Created External Stage
CREATE OR REPLACE STAGE CORETELECOMS_DW.RAW.S3_RAW_STAGE
    URL = 's3://coretelecoms-raw-zone-oluwakayode-dev/'
    STORAGE_INTEGRATION = CORETELECOMS_S3_INT
    FILE_FORMAT = CORETELECOMS_DW.RAW.PARQUET_FORMAT;

-- Customers (Static)
CREATE OR REPLACE TABLE CORETELECOMS_DW.RAW.CUSTOMERS (
    "customer_id" VARCHAR,
    "name" VARCHAR,
    "Gender" VARCHAR,
    "DATE of biRTH" VARCHAR,
    "signup_date" VARCHAR,
    "email" VARCHAR,
    "address" VARCHAR,
    "load_time" VARCHAR,
    _ingestion_time TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Agents (Static)
CREATE OR REPLACE TABLE CORETELECOMS_DW.RAW.AGENTS (
    "id" VARCHAR,
    "name" VARCHAR,
    "experience" VARCHAR,
    "state" VARCHAR,
    "load_time" VARCHAR,
    _ingestion_time TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Call Logs (Daily)
CREATE OR REPLACE TABLE CORETELECOMS_DW.RAW.CALL_CENTER_LOGS (
    "C0" VARCHAR, 
    "call ID" VARCHAR,
    "customeR iD" VARCHAR,
    "COMPLAINT_catego ry" VARCHAR,
    "agent ID" VARCHAR,
    "call_start_time" VARCHAR,
    "call_end_time" VARCHAR,
    "resolutionstatus" VARCHAR,
    "callLogsGenerationDate" VARCHAR,
    "load_time" VARCHAR,
    _ingestion_time TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Web Complaints (Daily)
CREATE OR REPLACE TABLE CORETELECOMS_DW.RAW.WEB_COMPLAINTS (
    "Column1" VARCHAR,
    "request_id" VARCHAR,
    "customeR iD" VARCHAR,
    "COMPLAINT_catego ry" VARCHAR,
    "agent ID" VARCHAR,
    "resolutionstatus" VARCHAR,
    "request_date" VARCHAR,
    "resolution_date" VARCHAR,
    "webFormGenerationDate" VARCHAR,
    "load_time" VARCHAR,
    _ingestion_time TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Social Media (Daily - Complex Struct)
-- Loading as Variant to avoid schema breaks
CREATE OR REPLACE TABLE CORETELECOMS_DW.RAW.SOCIAL_MEDIA (
    src_data VARIANT,
    _ingestion_time TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);