from airflow.sdk import dag, task_group, TriggerRule
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.smtp.operators.smtp import EmailOperator
from pendulum import datetime, duration

from assets import SNOWFLAKE_RAW_READY
from notifications import send_email_failure_alert

default_args = {
    'owner': 'Oluwakayode',
    'retries': 2,
    'retry_delay': duration(minutes=1),
    "on_failure_callback": send_email_failure_alert
}

@dag(
    dag_id="load_snowflake_pipeline",
    default_args=default_args,
    description="Triggered by S3 Ingestion. Loads Parquet to Snowflake Raw Layers",
    schedule=[SNOWFLAKE_RAW_READY],
    start_date=datetime(2025, 11, 20),
    catchup=False, 
    tags=["loading", "snowflake", "etl"]
)
def load_snowflake_pipeline():

    @task_group(group_id="load_static_tables")
    def load_static_tables():
        
        load_customers = SQLExecuteQueryOperator(
            task_id='load_customers',
            conn_id='snowflake_conn',
            split_statements=False, 
            sql="""
            BEGIN TRANSACTION;
                TRUNCATE TABLE CORE_TELECOMS_DW.RAW.CUSTOMERS;
                
                COPY INTO CORE_TELECOMS_DW.RAW.CUSTOMERS
                FROM @CORE_TELECOMS_DW.RAW.S3_RAW_STAGE/static/customers/
                FILE_FORMAT = (FORMAT_NAME = CORE_TELECOMS_DW.RAW.PARQUET_FORMAT)
                MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
                ON_ERROR = SKIP_FILE;
                
                -- Safety Check
                LET row_count INT := (SELECT COUNT(*) FROM CORE_TELECOMS_DW.RAW.CUSTOMERS);
                IF (row_count = 0) THEN
                    RAISE EXCEPTION 'Load Failed: No customers found in S3!';
                END IF;
            COMMIT;
            """
        )

        load_agents = SQLExecuteQueryOperator(
            task_id='load_agents',
            conn_id='snowflake_conn',
            split_statements=False,
            sql="""
            BEGIN TRANSACTION;
                TRUNCATE TABLE CORE_TELECOMS_DW.RAW.AGENTS;
                
                COPY INTO CORE_TELECOMS_DW.RAW.AGENTS
                FROM @CORE_TELECOMS_DW.RAW.S3_RAW_STAGE/static/agents/
                FILE_FORMAT = (FORMAT_NAME = CORE_TELECOMS_DW.RAW.PARQUET_FORMAT)
                MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
                ON_ERROR = SKIP_FILE;

                LET row_count INT := (SELECT COUNT(*) FROM CORE_TELECOMS_DW.RAW.AGENTS);
                IF (row_count = 0) THEN
                    RAISE EXCEPTION 'Load Failed: No agents found in S3!';
                END IF;
            COMMIT;
            """
        )

    @task_group(group_id="load_daily_tables")
    def load_daily_tables():
        
        load_call_logs = SQLExecuteQueryOperator(
            task_id='load_call_logs',
            conn_id='snowflake_conn',
            sql="""
                COPY INTO CORE_TELECOMS_DW.RAW.CALL_CENTER_LOGS
                FROM @CORE_TELECOMS_DW.RAW.S3_RAW_STAGE/call_logs/
                FILE_FORMAT = (FORMAT_NAME = CORE_TELECOMS_DW.RAW.PARQUET_FORMAT)
                MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
                ON_ERROR = SKIP_FILE;
            """
        )

        load_web_complaints = SQLExecuteQueryOperator(
            task_id='load_web_complaints',
            conn_id='snowflake_conn',
            sql="""
                COPY INTO CORE_TELECOMS_DW.RAW.WEB_COMPLAINTS
                FROM @CORE_TELECOMS_DW.RAW.S3_RAW_STAGE/web_complaints/
                FILE_FORMAT = (FORMAT_NAME = CORE_TELECOMS_DW.RAW.PARQUET_FORMAT)
                MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
                ON_ERROR = SKIP_FILE;
            """
        )

        load_social_media = SQLExecuteQueryOperator(
            task_id='load_social_media',
            conn_id='snowflake_conn',
            sql="""
                COPY INTO CORE_TELECOMS_DW.RAW.SOCIAL_MEDIA (src_data)
                FROM (
                    SELECT $1 
                    FROM @CORE_TELECOMS_DW.RAW.S3_RAW_STAGE/media_complaint/
                )
                FILE_FORMAT = (FORMAT_NAME = CORE_TELECOMS_DW.RAW.PARQUET_FORMAT)
                ON_ERROR = SKIP_FILE;
            """
        )

    send_success_email = EmailOperator(
        task_id="send_success_email",
        to="ojokayode13@gmail.com",
        subject="Success: Snowflake Load Completed ({{ ds }})",
        html_content="<h3>Snowflake Data Load Complete</h3><p>Raw tables updated.</p>",
        conn_id="smtp_conn",
        trigger_rule=TriggerRule.NONE_FAILED
    )

    [load_static_tables(), load_daily_tables()] >> send_success_email

load_snowflake_pipeline()