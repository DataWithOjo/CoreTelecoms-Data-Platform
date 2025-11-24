from airflow.sdk import dag, task_group, Variable, Asset
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.latest_only import LatestOnlyOperator
from pendulum import duration, datetime

from assets import S3_RAW_DATA_READY

default_args = {
    'owner': 'Oluwakayode',
    'retries': 2,
    'retry_delay': duration(minutes=1),
}

@dag(
    dag_id="ingestion_pipeline",
    default_args=default_args,
    description="Ingests Postgres, S3 Logs, and Customer Static Data",
    schedule="@daily", 
    start_date=datetime(2025, 11, 20),
    catchup=True, 
    tags=["ingestion", "elt"]
)
def ingestion_pipeline():

    RAW_BUCKET = Variable.get("S3_RAW_BUCKET")
    SOURCE_BUCKET = Variable.get("SOURCE_S3_BUCKET")

    @task_group(group_id="daily_ingestion")
    def daily_ingestion():
        
        extract_postgres = BashOperator(
            task_id="extract_postgres",
            bash_command="""
                python /opt/airflow/scripts/extract_postgres.py \
                --execution_date {{ ds }} \
                --target_bucket {{ params.bucket }}
            """,
            params={"bucket": RAW_BUCKET},
        )

        extract_call_logs = BashOperator(
            task_id="extract_call_logs",
            bash_command="""
                python /opt/airflow/scripts/extract_s3_data.py \
                --execution_date {{ ds }} \
                --source_bucket {{ params.src_bucket }} \
                --source_prefix "call_logs" \
                --target_bucket {{ params.tgt_bucket }} \
                --file_type csv
            """,
            params={"src_bucket": SOURCE_BUCKET, "tgt_bucket": RAW_BUCKET},
        )

        extract_social = BashOperator(
            task_id="extract_social_media",
            bash_command="""
                python /opt/airflow/scripts/extract_s3_data.py \
                --execution_date {{ ds }} \
                --source_bucket {{ params.src_bucket }} \
                --source_prefix "media_complaint" \
                --target_bucket {{ params.tgt_bucket }} \
                --file_type json
            """,
            params={"src_bucket": SOURCE_BUCKET, "tgt_bucket": RAW_BUCKET},
        )

    @task_group(group_id="static_ingestion")
    def static_ingestion():
        latest_only = LatestOnlyOperator(task_id="latest_only")

        extract_customers = BashOperator(
            task_id="extract_customers",
            bash_command="""
                python /opt/airflow/scripts/extract_s3_data.py \
                --execution_date "STATIC" \
                --source_bucket {{ params.src_bucket }} \
                --source_prefix "static" \
                --target_bucket {{ params.tgt_bucket }} \
                --file_type csv
            """,
            params={"src_bucket": SOURCE_BUCKET, "tgt_bucket": RAW_BUCKET},
        )

        extract_agents = BashOperator(
            task_id="extract_agents_gsheet",
            bash_command="""
                python /opt/airflow/scripts/extract_gsheets.py \
                --target_bucket {{ params.bucket }}
            """,
            params={"bucket": RAW_BUCKET},
        )

        latest_only >> [extract_customers, extract_agents]

    daily_group = daily_ingestion()
    static_group = static_ingestion()


    mark_done = EmptyOperator(
        task_id="mark_ingestion_complete",
        outlets=[S3_RAW_DATA_READY]
    )

    [daily_group, static_group] >> mark_done

ingestion_pipeline()