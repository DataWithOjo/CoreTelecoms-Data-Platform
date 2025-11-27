from airflow.sdk import dag, task_group, TriggerRule
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.snowflake.transfers.copy_into_snowflake import CopyFromExternalStageToSnowflakeOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.smtp.operators.smtp import EmailOperator
from pendulum import datetime, duration

from assets import S3_RAW_DATA_READY
from assets import SNOWFLAKE_RAW_READY
from notifications import send_email_failure_alert

default_args = {
    "owner": "Oluwakayode",
    "retries": 2,
    "retry_delay": duration(minutes=1),
    "conn_id": "snowflake_conn",
    "snowflake_conn_id": "snowflake_conn",
    "on_failure_callback": send_email_failure_alert
}

SNOWFLAKE_DB = "CORETELECOMS_DW"
SNOWFLAKE_SCHEMA = "RAW"
STAGE_NAME = "S3_RAW_STAGE"
FILE_FORMAT = "CORETELECOMS_DW.RAW.PARQUET_FORMAT"

STATIC_COPY_OPTIONS = "MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE ON_ERROR = SKIP_FILE FORCE = TRUE"
DAILY_COPY_OPTIONS = "MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE ON_ERROR = SKIP_FILE"

@dag(
    dag_id="load_snowflake_pipeline",
    default_args=default_args,
    description="Loads Parquet to Snowflake. Uses CLONE+SWAP for Static and MERGE for Daily.",
    schedule=[S3_RAW_DATA_READY],
    start_date=datetime(2025, 11, 20),
    catchup=False, 
    tags=["loading", "snowflake", "etl"]
)
def load_snowflake_pipeline():

    @task_group(group_id="load_static_tables")
    def load_static_tables():
        
        prep_customers_transient = SQLExecuteQueryOperator(
            task_id='prep_customers_transient',
            split_statements=True,
            sql=f"""
            ALTER SESSION SET QUERY_TAG = 'load_snowflake_pipeline.prep_customers';
            CREATE OR REPLACE TRANSIENT TABLE {SNOWFLAKE_DB}.{SNOWFLAKE_SCHEMA}.CUSTOMERS_TRANS 
            CLONE {SNOWFLAKE_DB}.{SNOWFLAKE_SCHEMA}.CUSTOMERS;
            TRUNCATE TABLE {SNOWFLAKE_DB}.{SNOWFLAKE_SCHEMA}.CUSTOMERS_TRANS;
            """
        )

        copy_customers = CopyFromExternalStageToSnowflakeOperator(
            task_id='copy_customers',
            database=SNOWFLAKE_DB,
            schema=SNOWFLAKE_SCHEMA,
            table="CUSTOMERS_TRANS", 
            stage=STAGE_NAME,
            prefix="raw/customers/",
            file_format=f"(FORMAT_NAME = {FILE_FORMAT})",
            copy_options=STATIC_COPY_OPTIONS,
        )

        swap_customers = SQLExecuteQueryOperator(
            task_id='swap_customers',
            split_statements=True,
            sql=f"""
            ALTER SESSION SET QUERY_TAG = 'load_snowflake_pipeline.swap_customers';
            UPDATE {SNOWFLAKE_DB}.{SNOWFLAKE_SCHEMA}.CUSTOMERS_TRANS 
            SET _ingestion_time = CURRENT_TIMESTAMP();
            
            ALTER TABLE {SNOWFLAKE_DB}.{SNOWFLAKE_SCHEMA}.CUSTOMERS 
            SWAP WITH {SNOWFLAKE_DB}.{SNOWFLAKE_SCHEMA}.CUSTOMERS_TRANS;
            """
        )

        prep_agents_transient = SQLExecuteQueryOperator(
            task_id='prep_agents_transient',
            split_statements=True,
            sql=f"""
            ALTER SESSION SET QUERY_TAG = 'load_snowflake_pipeline.prep_agents';
            CREATE OR REPLACE TRANSIENT TABLE {SNOWFLAKE_DB}.{SNOWFLAKE_SCHEMA}.AGENTS_TRANS 
            CLONE {SNOWFLAKE_DB}.{SNOWFLAKE_SCHEMA}.AGENTS;
            TRUNCATE TABLE {SNOWFLAKE_DB}.{SNOWFLAKE_SCHEMA}.AGENTS_TRANS;
            """
        )

        copy_agents = CopyFromExternalStageToSnowflakeOperator(
            task_id='copy_agents',
            database=SNOWFLAKE_DB,
            schema=SNOWFLAKE_SCHEMA,
            table="AGENTS_TRANS",
            stage=STAGE_NAME,
            prefix="raw/agents/",
            file_format=f"(FORMAT_NAME = {FILE_FORMAT})",
            copy_options=STATIC_COPY_OPTIONS,
        )

        swap_agents = SQLExecuteQueryOperator(
            task_id='swap_agents',
            split_statements=True,
            sql=f"""
            ALTER SESSION SET QUERY_TAG = 'load_snowflake_pipeline.swap_agents';
            UPDATE {SNOWFLAKE_DB}.{SNOWFLAKE_SCHEMA}.AGENTS_TRANS 
            SET _ingestion_time = CURRENT_TIMESTAMP();
            
            ALTER TABLE {SNOWFLAKE_DB}.{SNOWFLAKE_SCHEMA}.AGENTS 
            SWAP WITH {SNOWFLAKE_DB}.{SNOWFLAKE_SCHEMA}.AGENTS_TRANS;
            """
        )

        prep_customers_transient >> copy_customers >> swap_customers
        prep_agents_transient >> copy_agents >> swap_agents

    @task_group(group_id="load_daily_tables")
    def load_daily_tables():
        
        prep_call_logs_staging = SQLExecuteQueryOperator(
            task_id='prep_call_logs_staging',
            split_statements=True,
            sql=f"""
            ALTER SESSION SET QUERY_TAG = 'load_snowflake_pipeline.prep_call_logs';
            CREATE TRANSIENT TABLE IF NOT EXISTS {SNOWFLAKE_DB}.{SNOWFLAKE_SCHEMA}.CALL_LOGS_STAGING 
            CLONE {SNOWFLAKE_DB}.{SNOWFLAKE_SCHEMA}.CALL_CENTER_LOGS;
            TRUNCATE TABLE {SNOWFLAKE_DB}.{SNOWFLAKE_SCHEMA}.CALL_LOGS_STAGING;
            """
        )

        copy_call_logs = CopyFromExternalStageToSnowflakeOperator(
            task_id='copy_call_logs',
            database=SNOWFLAKE_DB,
            schema=SNOWFLAKE_SCHEMA,
            table="CALL_LOGS_STAGING",
            stage=STAGE_NAME,
            prefix="raw/call_logs/",
            file_format=f"(FORMAT_NAME = {FILE_FORMAT})",
            copy_options=DAILY_COPY_OPTIONS, 
        )
        
        merge_call_logs = SQLExecuteQueryOperator(
            task_id='merge_call_logs',
            split_statements=True,
            sql=f"""
            ALTER SESSION SET QUERY_TAG = 'load_snowflake_pipeline.merge_call_logs';
            ALTER SESSION SET ERROR_ON_NONDETERMINISTIC_MERGE = FALSE;
            ALTER SESSION SET STATEMENT_TIMEOUT_IN_SECONDS = 3600;

            UPDATE {SNOWFLAKE_DB}.{SNOWFLAKE_SCHEMA}.CALL_LOGS_STAGING 
            SET _ingestion_time = CURRENT_TIMESTAMP() 
            WHERE _ingestion_time IS NULL;

            MERGE INTO {SNOWFLAKE_DB}.{SNOWFLAKE_SCHEMA}.CALL_CENTER_LOGS AS TARGET
            USING (
                SELECT * FROM {SNOWFLAKE_DB}.{SNOWFLAKE_SCHEMA}.CALL_LOGS_STAGING
                QUALIFY ROW_NUMBER() OVER (PARTITION BY source_row_id ORDER BY _ingestion_time DESC) = 1
            ) AS SOURCE
            ON TARGET.source_row_id = SOURCE.source_row_id
            WHEN MATCHED THEN
                UPDATE SET 
                    TARGET.call_id = SOURCE.call_id,
                    TARGET.resolutionstatus = SOURCE.resolutionstatus,
                    TARGET.call_end_time = SOURCE.call_end_time,
                    TARGET._ingestion_time = SOURCE._ingestion_time
            WHEN NOT MATCHED THEN
                INSERT (SOURCE_ROW_ID, CALL_ID, CUSTOMER_ID, COMPLAINT_CATEGO_RY, AGENT_ID, CALL_START_TIME, CALL_END_TIME, RESOLUTIONSTATUS, CALLLOGSGENERATIONDATE, LOAD_TIME, _INGESTION_TIME)
                VALUES (SOURCE.SOURCE_ROW_ID, SOURCE.CALL_ID, SOURCE.CUSTOMER_ID, SOURCE.COMPLAINT_CATEGO_RY, SOURCE.AGENT_ID, SOURCE.CALL_START_TIME, SOURCE.CALL_END_TIME, SOURCE.RESOLUTIONSTATUS, SOURCE.CALLLOGSGENERATIONDATE, SOURCE.LOAD_TIME, SOURCE._INGESTION_TIME);
            """
        )

        prep_web_staging = SQLExecuteQueryOperator(
            task_id='prep_web_staging',
            split_statements=True,
            sql=f"""
            ALTER SESSION SET QUERY_TAG = 'load_snowflake_pipeline.prep_web_complaints';
            CREATE TRANSIENT TABLE IF NOT EXISTS {SNOWFLAKE_DB}.{SNOWFLAKE_SCHEMA}.WEB_COMPLAINTS_STAGING 
            CLONE {SNOWFLAKE_DB}.{SNOWFLAKE_SCHEMA}.WEB_COMPLAINTS;
            TRUNCATE TABLE {SNOWFLAKE_DB}.{SNOWFLAKE_SCHEMA}.WEB_COMPLAINTS_STAGING;
            """
        )

        copy_web_complaints = SQLExecuteQueryOperator(
            task_id='copy_web_complaints',
            split_statements=True,
            sql=f"""
            COPY INTO {SNOWFLAKE_DB}.{SNOWFLAKE_SCHEMA}.WEB_COMPLAINTS_STAGING 
            (
                SOURCE_ROW_ID, REQUEST_ID, CUSTOMER_ID, COMPLAINT_CATEGO_RY, 
                AGENT_ID, RESOLUTIONSTATUS, REQUEST_DATE, RESOLUTION_DATE, 
                WEBFORMGENERATIONDATE, LOAD_TIME
            )
            FROM (
                SELECT 
                    COALESCE($1:source_row_id, $1:SOURCE_ROW_ID, $1:Source_Row_Id)::VARCHAR, 
                    $1:request_id::VARCHAR, 
                    $1:customer_id::VARCHAR, 
                    $1:complaint_catego_ry::VARCHAR, 
                    $1:agent_id::VARCHAR, 
                    $1:resolutionstatus::VARCHAR, 
                    $1:request_date::VARCHAR, 
                    $1:resolution_date::VARCHAR, 
                    $1:webformgenerationdate::VARCHAR, 
                    $1:load_time::VARCHAR
                FROM @{SNOWFLAKE_DB}.{SNOWFLAKE_SCHEMA}.{STAGE_NAME}/raw/postgres/
            )
            FILE_FORMAT = (FORMAT_NAME = {FILE_FORMAT})
            ON_ERROR = SKIP_FILE
            """
        )

        merge_web_complaints = SQLExecuteQueryOperator(
            task_id='merge_web_complaints',
            split_statements=True,
            sql=f"""
            ALTER SESSION SET QUERY_TAG = 'load_snowflake_pipeline.merge_web_complaints';
            ALTER SESSION SET ERROR_ON_NONDETERMINISTIC_MERGE = FALSE;
            ALTER SESSION SET STATEMENT_TIMEOUT_IN_SECONDS = 3600;

            UPDATE {SNOWFLAKE_DB}.{SNOWFLAKE_SCHEMA}.WEB_COMPLAINTS_STAGING 
            SET _ingestion_time = CURRENT_TIMESTAMP() 
            WHERE _ingestion_time IS NULL;

            MERGE INTO {SNOWFLAKE_DB}.{SNOWFLAKE_SCHEMA}.WEB_COMPLAINTS AS TARGET
            USING (
                SELECT * FROM {SNOWFLAKE_DB}.{SNOWFLAKE_SCHEMA}.WEB_COMPLAINTS_STAGING
                QUALIFY ROW_NUMBER() OVER (PARTITION BY request_id ORDER BY _ingestion_time DESC) = 1
            ) AS SOURCE
            ON TARGET.request_id = SOURCE.request_id
            WHEN MATCHED THEN
                -- FIX: Now updates ALL fields, ensuring NULLs in existing rows are overwritten
                UPDATE SET 
                    TARGET.source_row_id = SOURCE.source_row_id,
                    TARGET.customer_id = SOURCE.customer_id,
                    TARGET.complaint_catego_ry = SOURCE.complaint_catego_ry,
                    TARGET.agent_id = SOURCE.agent_id,
                    TARGET.resolutionstatus = SOURCE.resolutionstatus,
                    TARGET.request_date = SOURCE.request_date,
                    TARGET.resolution_date = SOURCE.resolution_date,
                    TARGET.webformgenerationdate = SOURCE.webformgenerationdate,
                    TARGET.load_time = SOURCE.load_time,
                    TARGET._ingestion_time = SOURCE._ingestion_time
            WHEN NOT MATCHED THEN
                INSERT (SOURCE_ROW_ID, REQUEST_ID, CUSTOMER_ID, COMPLAINT_CATEGO_RY, AGENT_ID, RESOLUTIONSTATUS, REQUEST_DATE, RESOLUTION_DATE, WEBFORMGENERATIONDATE, LOAD_TIME, _INGESTION_TIME)
                VALUES (SOURCE.SOURCE_ROW_ID, SOURCE.REQUEST_ID, SOURCE.CUSTOMER_ID, SOURCE.COMPLAINT_CATEGO_RY, SOURCE.AGENT_ID, SOURCE.RESOLUTIONSTATUS, SOURCE.REQUEST_DATE, SOURCE.RESOLUTION_DATE, SOURCE.WEBFORMGENERATIONDATE, SOURCE.LOAD_TIME, SOURCE._INGESTION_TIME);
            """
        )

        prep_social_staging = SQLExecuteQueryOperator(
            task_id='prep_social_staging',
            split_statements=True,
            sql=f"""
            ALTER SESSION SET QUERY_TAG = 'load_snowflake_pipeline.prep_social_media';
            CREATE TRANSIENT TABLE IF NOT EXISTS {SNOWFLAKE_DB}.{SNOWFLAKE_SCHEMA}.SOCIAL_MEDIA_STAGING 
            CLONE {SNOWFLAKE_DB}.{SNOWFLAKE_SCHEMA}.SOCIAL_MEDIA;
            TRUNCATE TABLE {SNOWFLAKE_DB}.{SNOWFLAKE_SCHEMA}.SOCIAL_MEDIA_STAGING;
            """
        )

        copy_social_media = SQLExecuteQueryOperator(
            task_id='copy_social_media',
            split_statements=True,
            sql=f"""
            COPY INTO {SNOWFLAKE_DB}.{SNOWFLAKE_SCHEMA}.SOCIAL_MEDIA_STAGING 
            (
                COMPLAINT_ID, CUSTOMER_ID, COMPLAINT_CATEGO_RY, AGENT_ID, 
                RESOLUTIONSTATUS, REQUEST_DATE, RESOLUTION_DATE, MEDIA_CHANNEL, 
                MEDIACOMPLAINTGENERATIONDATE, LOAD_TIME
            )
            FROM (
                SELECT 
                    COALESCE($1:complaint_id, $1:COMPLAINT_ID, $1:Complaint_Id)::VARCHAR, 
                    $1:customer_id::VARCHAR, 
                    $1:complaint_catego_ry::VARCHAR, 
                    $1:agent_id::VARCHAR, 
                    $1:resolutionstatus::VARCHAR, 
                    $1:request_date::VARCHAR, 
                    $1:resolution_date::VARCHAR, 
                    $1:media_channel::VARCHAR, 
                    $1:mediacomplaintgenerationdate::VARCHAR, 
                    $1:load_time::VARCHAR
                FROM @{SNOWFLAKE_DB}.{SNOWFLAKE_SCHEMA}.{STAGE_NAME}/raw/social_media/
            )
            FILE_FORMAT = (FORMAT_NAME = {FILE_FORMAT})
            ON_ERROR = SKIP_FILE
            """
        )

        merge_social_media = SQLExecuteQueryOperator(
            task_id='merge_social_media',
            split_statements=True,
            sql=f"""
            ALTER SESSION SET QUERY_TAG = 'load_snowflake_pipeline.merge_social_media';
            ALTER SESSION SET ERROR_ON_NONDETERMINISTIC_MERGE = FALSE;
            ALTER SESSION SET STATEMENT_TIMEOUT_IN_SECONDS = 3600;

            UPDATE {SNOWFLAKE_DB}.{SNOWFLAKE_SCHEMA}.SOCIAL_MEDIA_STAGING 
            SET _ingestion_time = CURRENT_TIMESTAMP() 
            WHERE _ingestion_time IS NULL;

            MERGE INTO {SNOWFLAKE_DB}.{SNOWFLAKE_SCHEMA}.SOCIAL_MEDIA AS TARGET
            USING (
                SELECT * FROM {SNOWFLAKE_DB}.{SNOWFLAKE_SCHEMA}.SOCIAL_MEDIA_STAGING
                QUALIFY ROW_NUMBER() OVER (PARTITION BY complaint_id ORDER BY _ingestion_time DESC) = 1
            ) AS SOURCE
            ON TARGET.complaint_id = SOURCE.complaint_id
            WHEN MATCHED THEN
                UPDATE SET 
                    TARGET.customer_id = SOURCE.customer_id,
                    TARGET.complaint_catego_ry = SOURCE.complaint_catego_ry,
                    TARGET.agent_id = SOURCE.agent_id,
                    TARGET.resolutionstatus = SOURCE.resolutionstatus,
                    TARGET.request_date = SOURCE.request_date,
                    TARGET.resolution_date = SOURCE.resolution_date,
                    TARGET.media_channel = SOURCE.media_channel,
                    TARGET.mediacomplaintgenerationdate = SOURCE.mediacomplaintgenerationdate,
                    TARGET.load_time = SOURCE.load_time,
                    TARGET._ingestion_time = SOURCE._ingestion_time
            WHEN NOT MATCHED THEN
                INSERT (COMPLAINT_ID, CUSTOMER_ID, COMPLAINT_CATEGO_RY, AGENT_ID, RESOLUTIONSTATUS, REQUEST_DATE, RESOLUTION_DATE, MEDIA_CHANNEL, MEDIACOMPLAINTGENERATIONDATE, LOAD_TIME, _INGESTION_TIME)
                VALUES (SOURCE.COMPLAINT_ID, SOURCE.CUSTOMER_ID, SOURCE.COMPLAINT_CATEGO_RY, SOURCE.AGENT_ID, SOURCE.RESOLUTIONSTATUS, SOURCE.REQUEST_DATE, SOURCE.RESOLUTION_DATE, SOURCE.MEDIA_CHANNEL, SOURCE.MEDIACOMPLAINTGENERATIONDATE, SOURCE.LOAD_TIME, SOURCE._INGESTION_TIME);
            """
        )

        prep_call_logs_staging >> copy_call_logs >> merge_call_logs
        prep_web_staging >> copy_web_complaints >> merge_web_complaints
        prep_social_staging >> copy_social_media >> merge_social_media

    mark_done = EmptyOperator(
        task_id="mark_loading_complete",
        trigger_rule=TriggerRule.NONE_FAILED,
        outlets=[SNOWFLAKE_RAW_READY]
    )    

    send_success_email = EmailOperator(
        task_id="send_success_email",
        to="ojokayode13@gmail.com",
        subject="Success: Snowflake Load Completed",
        html_content="<h3>Data Loaded to Snowflake</h3><p>Raw tables updated.</p>",
        conn_id="smtp_conn"
    )

    [load_static_tables(), load_daily_tables()] >> mark_done >> send_success_email

load_snowflake_pipeline()