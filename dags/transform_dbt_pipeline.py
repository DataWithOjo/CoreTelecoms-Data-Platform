from airflow.sdk import dag, TriggerRule
from airflow.providers.smtp.operators.smtp import EmailOperator
from pendulum import datetime
import os
from pathlib import Path

from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig
from cosmos.profiles import SnowflakeUserPasswordProfileMapping

from assets import SNOWFLAKE_RAW_READY
from notifications import send_email_failure_alert

DBT_PROJECT_PATH = Path("/opt/airflow/dbt_core_telecoms")

DBT_EXECUTABLE_PATH = "/home/airflow/.local/bin/dbt"

profile_config = ProfileConfig(
    profile_name="core_telecoms",
    target_name="dev",
    profile_mapping=SnowflakeUserPasswordProfileMapping(
        conn_id="snowflake_default", 
        profile_args={"schema": "ANALYTICS"},
    )
)

@dag(
    dag_id="transform_dbt_pipeline",
    start_date=datetime(2025, 11, 20),
    schedule=[SNOWFLAKE_RAW_READY],
    catchup=False,
    tags=["transformation", "dbt", "cosmos"],
    on_failure_callback=send_email_failure_alert
)
def transform_pipeline():
    
    transform_data = DbtTaskGroup(
        group_id="dbt_transformation",
        project_config=ProjectConfig(DBT_PROJECT_PATH),
        profile_config=profile_config,
        execution_config=ExecutionConfig(
            dbt_executable_path=DBT_EXECUTABLE_PATH,
        ),
        operator_args={
            "install_deps": True,
        }
    )

    email_alert = EmailOperator(
        task_id="send_success_email",
        to="ojokayode13@gmail.com",
        subject="Success: Data Transformation Completed",
        html_content="<h3>dbt Transformation Complete</h3><p>Unified Marts are ready for reporting.</p>",
        conn_id="smtp_conn",
        trigger_rule=TriggerRule.NONE_FAILED
    )

    transform_data >> email_alert

transform_pipeline()