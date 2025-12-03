USE SCHEMA CORETELECOMS_DW.RAW;

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