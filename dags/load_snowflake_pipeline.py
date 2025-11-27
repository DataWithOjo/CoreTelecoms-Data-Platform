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
    'snowflake_conn_id': 'snowflake_conn',
    "on_failure_callback": send_email_failure_alert
}

SNOWFLAKE_DB = "CORETELECOMS_DW"
SNOWFLAKE_SCHEMA = "RAW"
STAGE_NAME = "S3_RAW_STAGE"
FILE_FORMAT = "CORETELECOMS_DW.RAW.PARQUET_FORMAT"

DEFAULT_COPY_OPTIONS = "MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE ON_ERROR = SKIP_FILE"

@dag(
    dag_id="load_snowflake_pipeline",
    default_args=default_args,
    description="Triggered by S3 Ingestion. Loads Parquet to Snowflake Raw Layers",
    schedule=[S3_RAW_DATA_READY],
    start_date=datetime(2025, 11, 20),
    catchup=False, 
    tags=["loading", "snowflake", "etl"]
)
def load_snowflake_pipeline():

    @task_group(group_id="load_static_tables")
    def load_static_tables():
        
        # --- CUSTOMERS ---
        delete_customers = SQLExecuteQueryOperator(
            task_id='delete_customers',
            sql=f"DELETE FROM {SNOWFLAKE_DB}.{SNOWFLAKE_SCHEMA}.CUSTOMERS",
            split_statements=False,
        )

        copy_customers = CopyFromExternalStageToSnowflakeOperator(
            task_id='copy_customers',
            database=SNOWFLAKE_DB,
            schema=SNOWFLAKE_SCHEMA,
            table="CUSTOMERS",
            stage=STAGE_NAME,
            prefix="raw/customers/",
            file_format=f"(FORMAT_NAME = {FILE_FORMAT})",
            copy_options=DEFAULT_COPY_OPTIONS,
        )

        update_customers_ts = SQLExecuteQueryOperator(
            task_id='update_customers_ts',
            sql=f"""
            UPDATE {SNOWFLAKE_DB}.{SNOWFLAKE_SCHEMA}.CUSTOMERS 
            SET _ingestion_time = CURRENT_TIMESTAMP() 
            WHERE _ingestion_time IS NULL
            """
        )

        # --- AGENTS ---
        delete_agents = SQLExecuteQueryOperator(
            task_id='delete_agents',
            sql=f"DELETE FROM {SNOWFLAKE_DB}.{SNOWFLAKE_SCHEMA}.AGENTS",
            split_statements=False,
        )

        copy_agents = CopyFromExternalStageToSnowflakeOperator(
            task_id='copy_agents',
            database=SNOWFLAKE_DB,
            schema=SNOWFLAKE_SCHEMA,
            table="AGENTS",
            stage=STAGE_NAME,
            prefix="raw/agents/",
            file_format=f"(FORMAT_NAME = {FILE_FORMAT})",
            copy_options=DEFAULT_COPY_OPTIONS,
        )

        update_agents_ts = SQLExecuteQueryOperator(
            task_id='update_agents_ts',
            sql=f"""
            UPDATE {SNOWFLAKE_DB}.{SNOWFLAKE_SCHEMA}.AGENTS 
            SET _ingestion_time = CURRENT_TIMESTAMP() 
            WHERE _ingestion_time IS NULL
            """
        )

        delete_customers >> copy_customers >> update_customers_ts
        delete_agents >> copy_agents >> update_agents_ts

    @task_group(group_id="load_daily_tables")
    def load_daily_tables():
        
        # Updated:
        # 1. Added FORCE=TRUE so Snowflake doesn't ignore the file you are testing.
        # 2. Added COALESCE to handle 'source_row_id', 'SOURCE_ROW_ID', or 'Source_Row_Id'.
        # 3. Added explicit ::VARCHAR casting.
        load_call_logs = SQLExecuteQueryOperator(
            task_id='load_call_logs',
            split_statements=False,
            sql=f"""
            COPY INTO {SNOWFLAKE_DB}.{SNOWFLAKE_SCHEMA}.CALL_CENTER_LOGS 
            (
                SOURCE_ROW_ID, CALL_ID, CUSTOMER_ID, COMPLAINT_CATEGO_RY, 
                AGENT_ID, CALL_START_TIME, CALL_END_TIME, RESOLUTIONSTATUS, 
                CALLLOGSGENERATIONDATE, LOAD_TIME
            )
            FROM (
                SELECT 
                    COALESCE($1:source_row_id, $1:SOURCE_ROW_ID, $1:Source_Row_Id)::VARCHAR, 
                    $1:call_id::VARCHAR, 
                    $1:customer_id::VARCHAR, 
                    $1:complaint_catego_ry::VARCHAR, 
                    $1:agent_id::VARCHAR, 
                    $1:call_start_time::VARCHAR, 
                    $1:call_end_time::VARCHAR, 
                    $1:resolutionstatus::VARCHAR, 
                    $1:calllogsgenerationdate::VARCHAR, 
                    $1:load_time::VARCHAR
                FROM @{SNOWFLAKE_DB}.{SNOWFLAKE_SCHEMA}.{STAGE_NAME}/raw/call_logs/
            )
            FILE_FORMAT = (FORMAT_NAME = {FILE_FORMAT})
            ON_ERROR = SKIP_FILE
            FORCE = TRUE
            """
        )
        
        update_call_logs_ts = SQLExecuteQueryOperator(
            task_id='update_call_logs_ts',
            sql=f"""
            UPDATE {SNOWFLAKE_DB}.{SNOWFLAKE_SCHEMA}.CALL_CENTER_LOGS 
            SET _ingestion_time = CURRENT_TIMESTAMP() 
            WHERE _ingestion_time IS NULL
            """
        )

        # --- WEB COMPLAINTS ---
        load_web_complaints = SQLExecuteQueryOperator(
            task_id='load_web_complaints',
            split_statements=False,
            sql=f"""
            COPY INTO {SNOWFLAKE_DB}.{SNOWFLAKE_SCHEMA}.WEB_COMPLAINTS
            FROM @{SNOWFLAKE_DB}.{SNOWFLAKE_SCHEMA}.{STAGE_NAME}/raw/postgres/
            FILE_FORMAT = (FORMAT_NAME = {FILE_FORMAT})
            MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
            ON_ERROR = SKIP_FILE
            FORCE = TRUE
            """
        )

        update_web_complaints_ts = SQLExecuteQueryOperator(
            task_id='update_web_complaints_ts',
            sql=f"""
            UPDATE {SNOWFLAKE_DB}.{SNOWFLAKE_SCHEMA}.WEB_COMPLAINTS 
            SET _ingestion_time = CURRENT_TIMESTAMP() 
            WHERE _ingestion_time IS NULL
            """
        )

        # --- SOCIAL MEDIA ---
        load_social_media = SQLExecuteQueryOperator(
            task_id='load_social_media',
            split_statements=False,
            sql=f"""
            COPY INTO {SNOWFLAKE_DB}.{SNOWFLAKE_SCHEMA}.SOCIAL_MEDIA
            FROM @{SNOWFLAKE_DB}.{SNOWFLAKE_SCHEMA}.{STAGE_NAME}/raw/social_media/
            FILE_FORMAT = (FORMAT_NAME = {FILE_FORMAT})
            MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
            ON_ERROR = SKIP_FILE
            FORCE = TRUE
            """
        )

        update_social_media_ts = SQLExecuteQueryOperator(
            task_id='update_social_media_ts',
            sql=f"""
            UPDATE {SNOWFLAKE_DB}.{SNOWFLAKE_SCHEMA}.SOCIAL_MEDIA 
            SET _ingestion_time = CURRENT_TIMESTAMP() 
            WHERE _ingestion_time IS NULL
            """
        )

        load_call_logs >> update_call_logs_ts
        load_web_complaints >> update_web_complaints_ts
        load_social_media >> update_social_media_ts

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