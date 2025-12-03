from airflow.sdk import dag, task_group, TriggerRule
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.latest_only import LatestOnlyOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.smtp.operators.smtp import EmailOperator
from pendulum import duration, datetime

from assets import S3_RAW_DATA_READY
from notifications import send_email_failure_alert

default_args = {
    'owner': 'Oluwakayode',
    'retries': 2,
    'retry_delay': duration(minutes=1),
    "on_failure_callback": send_email_failure_alert
}

@dag(
    dag_id="ingestion_pipeline",
    default_args=default_args,
    description="Ingests Postgres, S3 Logs, Google Sheet and Customer Static Data",
    schedule="@daily", 
    start_date=datetime(2025, 11, 20),
    catchup=True, 
    tags=["ingestion", "etl"]
)
def ingestion_pipeline():

    DEFAULT_RAW_BUCKET = "coretelecoms-raw-zone-oluwakayode-dev"
    DEFAULT_SOURCE_BUCKET = "core-telecoms-data-lake"

    @task_group(group_id="daily_ingestion")
    def daily_ingestion():
        
        extract_postgres = BashOperator(
            task_id="extract_postgres",
            bash_command=f"""
                python /opt/airflow/scripts/extract_postgres.py \
                --execution_date {{{{ ds }}}} \
                --target_bucket {{{{ var.value.get('S3_RAW_BUCKET', '{DEFAULT_RAW_BUCKET}') }}}}
            """
        )
        
        extract_call_logs = BashOperator(
            task_id="extract_call_logs",
            bash_command=f"""
                python /opt/airflow/scripts/extract_s3_data.py \
                --execution_date {{{{ ds }}}} \
                --source_bucket {{{{ var.value.get('SOURCE_S3_BUCKET', '{DEFAULT_SOURCE_BUCKET}') }}}} \
                --source_prefix "call_logs" \
                --target_bucket {{{{ var.value.get('S3_RAW_BUCKET', '{DEFAULT_RAW_BUCKET}') }}}} \
                --file_type csv
            """
        )

        extract_social = BashOperator(
            task_id="extract_social_media",
            bash_command=f"""
                python /opt/airflow/scripts/extract_s3_data.py \
                --execution_date {{{{ ds }}}} \
                --source_bucket {{{{ var.value.get('SOURCE_S3_BUCKET', '{DEFAULT_SOURCE_BUCKET}') }}}} \
                --source_prefix "media_complaint" \
                --target_bucket {{{{ var.value.get('S3_RAW_BUCKET', '{DEFAULT_RAW_BUCKET}') }}}} \
                --file_type json
            """
        )

    @task_group(group_id="static_ingestion")
    def static_ingestion():
        latest_only = LatestOnlyOperator(task_id="latest_only")

        extract_customers = BashOperator(
            task_id="extract_customers",
            bash_command=f"""
                python /opt/airflow/scripts/extract_s3_data.py \
                --execution_date "STATIC" \
                --source_bucket {{{{ var.value.get('SOURCE_S3_BUCKET', '{DEFAULT_SOURCE_BUCKET}') }}}} \
                --source_prefix "static" \
                --target_bucket {{{{ var.value.get('S3_RAW_BUCKET', '{DEFAULT_RAW_BUCKET}') }}}} \
                --file_type csv
            """
        )

        extract_agents = BashOperator(
            task_id="extract_agents_gsheet",
            bash_command=f"""
                python /opt/airflow/scripts/extract_gsheets.py \
                --target_bucket {{{{ var.value.get('S3_RAW_BUCKET', '{DEFAULT_RAW_BUCKET}') }}}}
            """
        )

        latest_only >> [extract_customers, extract_agents]

    daily_group = daily_ingestion()
    static_group = static_ingestion()

    mark_done = EmptyOperator(
        task_id="mark_ingestion_complete",
        trigger_rule=TriggerRule.NONE_FAILED,
        outlets=[S3_RAW_DATA_READY]
    )

    send_success_email = EmailOperator(
        task_id="send_success_email",
        to="ojokayode13@gmail.com",
        subject="Success: Ingestion Pipeline Completed ({{ ds }})",
        html_content="<h3>Pipeline Finished</h3><p>All data for {{ ds }} has been ingested successfully.</p>",
        conn_id="smtp_conn",
        trigger_rule=TriggerRule.NONE_FAILED
    )

    [daily_group, static_group] >> mark_done >> send_success_email

ingestion_pipeline()