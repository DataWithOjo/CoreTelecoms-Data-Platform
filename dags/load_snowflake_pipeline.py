from airflow.sdk import dag, task_group, TriggerRule
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.smtp.operators.smtp import EmailOperator
from pendulum import datetime, duration

from assets import S3_RAW_DATA_READY
from notifications import send_email_failure_alert

default_args = {
    'owner': 'Oluwakayode',
    'retries': 2,
    'retry_delay': duration(minutes=1),
    'conn_id': 'snowflake_conn',
    "on_failure_callback": send_email_failure_alert
}

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
        
        load_customers = SQLExecuteQueryOperator(
            task_id='load_customers',
            split_statements=False, 
            sql="""
            EXECUTE IMMEDIATE $$
            BEGIN
                BEGIN TRANSACTION;
                DELETE FROM CORETELECOMS_DW.RAW.CUSTOMERS;
                
                COPY INTO CORETELECOMS_DW.RAW.CUSTOMERS
                FROM @CORETELECOMS_DW.RAW.S3_RAW_STAGE/raw/customers/
                FILE_FORMAT = (FORMAT_NAME = CORETELECOMS_DW.RAW.PARQUET_FORMAT)
                MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
                ON_ERROR = SKIP_FILE
                FORCE = TRUE; 
                
                UPDATE CORETELECOMS_DW.RAW.CUSTOMERS 
                SET _ingestion_time = CURRENT_TIMESTAMP() 
                WHERE _ingestion_time IS NULL;

                LET rows_loaded INTEGER := SQLROWCOUNT;
                IF (rows_loaded = 0) THEN
                    LET load_failed EXCEPTION (-20001, 'Load Failed: No customers found!');
                    RAISE load_failed;
                END IF;
                
                COMMIT;
                RETURN 'Success: Loaded ' || rows_loaded || ' rows.';
            EXCEPTION
                WHEN OTHER THEN
                    ROLLBACK;
                    RAISE;
            END;
            $$
            """
        )

        load_agents = SQLExecuteQueryOperator(
            task_id='load_agents',
            split_statements=False,
            sql="""
            EXECUTE IMMEDIATE $$
            BEGIN
                BEGIN TRANSACTION;
                DELETE FROM CORETELECOMS_DW.RAW.AGENTS;
                
                COPY INTO CORETELECOMS_DW.RAW.AGENTS
                FROM @CORETELECOMS_DW.RAW.S3_RAW_STAGE/raw/agents/
                FILE_FORMAT = (FORMAT_NAME = CORETELECOMS_DW.RAW.PARQUET_FORMAT)
                MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
                ON_ERROR = SKIP_FILE
                FORCE = TRUE;

                UPDATE CORETELECOMS_DW.RAW.AGENTS 
                SET _ingestion_time = CURRENT_TIMESTAMP() 
                WHERE _ingestion_time IS NULL;

                LET rows_loaded INTEGER := SQLROWCOUNT;
                IF (rows_loaded = 0) THEN
                    LET load_failed EXCEPTION (-20001, 'Load Failed: No agents found!');
                    RAISE load_failed;
                END IF;

                COMMIT;
                RETURN 'Success: Loaded ' || rows_loaded || ' rows.';
            EXCEPTION
                WHEN OTHER THEN
                    ROLLBACK;
                    RAISE;
            END;
            $$
            """
        )

    @task_group(group_id="load_daily_tables")
    def load_daily_tables():
        
        load_call_logs = SQLExecuteQueryOperator(
            task_id='load_call_logs',
            split_statements=False,
            sql="""
            EXECUTE IMMEDIATE $$
            BEGIN
                BEGIN TRANSACTION;
                
                COPY INTO CORETELECOMS_DW.RAW.CALL_CENTER_LOGS
                FROM @CORETELECOMS_DW.RAW.S3_RAW_STAGE/raw/call_logs/
                FILE_FORMAT = (FORMAT_NAME = CORETELECOMS_DW.RAW.PARQUET_FORMAT)
                MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
                ON_ERROR = SKIP_FILE;
                
                UPDATE CORETELECOMS_DW.RAW.CALL_CENTER_LOGS 
                SET _ingestion_time = CURRENT_TIMESTAMP() 
                WHERE _ingestion_time IS NULL;

                LET rows_loaded INTEGER := SQLROWCOUNT;
                COMMIT;
                RETURN 'Success: Loaded ' || rows_loaded || ' rows.';
            EXCEPTION
                WHEN OTHER THEN
                    ROLLBACK;
                    RAISE;
            END;
            $$
            """
        )

        load_web_complaints = SQLExecuteQueryOperator(
            task_id='load_web_complaints',
            split_statements=False,
            sql="""
            EXECUTE IMMEDIATE $$
            BEGIN
                BEGIN TRANSACTION;
                
                COPY INTO CORETELECOMS_DW.RAW.WEB_COMPLAINTS
                FROM @CORETELECOMS_DW.RAW.S3_RAW_STAGE/raw/postgres/
                FILE_FORMAT = (FORMAT_NAME = CORETELECOMS_DW.RAW.PARQUET_FORMAT)
                MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
                ON_ERROR = SKIP_FILE;

                UPDATE CORETELECOMS_DW.RAW.WEB_COMPLAINTS 
                SET _ingestion_time = CURRENT_TIMESTAMP() 
                WHERE _ingestion_time IS NULL;

                LET rows_loaded INTEGER := SQLROWCOUNT;
                COMMIT;
                RETURN 'Success: Loaded ' || rows_loaded || ' rows.';
            EXCEPTION
                WHEN OTHER THEN
                    ROLLBACK;
                    RAISE;
            END;
            $$
            """
        )

        load_social_media = SQLExecuteQueryOperator(
            task_id='load_social_media',
            split_statements=False,
            sql="""
            EXECUTE IMMEDIATE $$
            BEGIN
                BEGIN TRANSACTION;
                
                COPY INTO CORETELECOMS_DW.RAW.SOCIAL_MEDIA
                FROM @CORETELECOMS_DW.RAW.S3_RAW_STAGE/raw/social_media/
                FILE_FORMAT = (FORMAT_NAME = CORETELECOMS_DW.RAW.PARQUET_FORMAT)
                MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
                ON_ERROR = SKIP_FILE;
                
                UPDATE CORETELECOMS_DW.RAW.SOCIAL_MEDIA 
                SET _ingestion_time = CURRENT_TIMESTAMP() 
                WHERE _ingestion_time IS NULL;

                LET rows_loaded INTEGER := SQLROWCOUNT;
                COMMIT;
                RETURN 'Success: Loaded ' || rows_loaded || ' rows.';
            EXCEPTION
                WHEN OTHER THEN
                    ROLLBACK;
                    RAISE;
            END;
            $$
            """
        )

    send_success_email = EmailOperator(
        task_id="send_success_email",
        to="ojokayode13@gmail.com",
        subject="Success: Snowflake Load Completed",
        html_content="<h3>Data Loaded to Snowflake</h3><p>Raw tables updated.</p>",
        conn_id="smtp_conn",
        trigger_rule=TriggerRule.NONE_FAILED
    )

    [load_static_tables(), load_daily_tables()] >> send_success_email

load_snowflake_pipeline()