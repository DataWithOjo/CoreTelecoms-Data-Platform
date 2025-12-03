import pytest
from airflow.models import DagBag
from airflow.utils.dag_cycle_tester import check_cycle
from airflow.providers.snowflake.transfers.copy_into_snowflake import CopyFromExternalStageToSnowflakeOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

DAG_ID = "load_snowflake_pipeline"

@pytest.fixture()
def dagbag():
    """Create a DagBag instance to load DAGs from the file."""
    return DagBag(dag_folder="dags/load_snowflake_pipeline.py", include_examples=False)

def test_dag_loaded(dagbag):
    """Ensure the DAG loads correctly with no import errors."""
    dag = dagbag.get_dag(dag_id=DAG_ID)
    
    assert dagbag.import_errors == {}, f"Import errors: {dagbag.import_errors}"
    assert dag is not None
    assert len(dag.tasks) > 0

def test_dag_structure(dagbag):
    """Ensure specific tasks exist in the DAG."""
    dag = dagbag.get_dag(dag_id=DAG_ID)
    task_ids = list(dag.task_dict.keys())
    
    expected_tasks = [
        # Static Group
        "load_static_tables.prep_customers_transient",
        "load_static_tables.copy_customers",
        "load_static_tables.swap_customers",
        "load_static_tables.prep_agents_transient",
        "load_static_tables.copy_agents",
        "load_static_tables.swap_agents",
        
        # Daily Group
        "load_daily_tables.prep_call_logs_staging",
        "load_daily_tables.copy_call_logs",
        "load_daily_tables.merge_call_logs",
        
        "load_daily_tables.prep_web_staging",
        "load_daily_tables.copy_web_complaints",
        "load_daily_tables.merge_web_complaints",
        
        "load_daily_tables.prep_social_staging",
        "load_daily_tables.copy_social_media",
        "load_daily_tables.merge_social_media",
        
        # End Tasks
        "mark_loading_complete",
        "send_success_email"
    ]
    
    for task_id in expected_tasks:
        assert task_id in task_ids, f"Task {task_id} missing from DAG"

def test_no_cycles(dagbag):
    """Ensure the DAG has no circular dependencies."""
    dag = dagbag.get_dag(dag_id=DAG_ID)
    check_cycle(dag)

def test_static_load_configuration(dagbag):
    """
    Verify that Static Tables use FORCE=TRUE in their Copy Options.
    """
    dag = dagbag.get_dag(dag_id=DAG_ID)
    
    copy_task = dag.get_task("load_static_tables.copy_customers")
    assert isinstance(copy_task, CopyFromExternalStageToSnowflakeOperator)
    
    copy_options = copy_task.copy_options.upper()
    assert "FORCE = TRUE" in copy_options
    assert "MATCH_BY_COLUMN_NAME" in copy_options

def test_static_swap_logic(dagbag):
    """
    Verify that Static Tables use the SWAP pattern in SQL.
    """
    dag = dagbag.get_dag(dag_id=DAG_ID)
    
    swap_task = dag.get_task("load_static_tables.swap_customers")
    assert isinstance(swap_task, SQLExecuteQueryOperator)
    
    sql = swap_task.sql.upper()
    assert "SWAP WITH" in sql
    assert "UPDATE" in sql

def test_daily_load_merge_logic(dagbag):
    """
    Verify that Daily Tables use MERGE for idempotency.
    """
    dag = dagbag.get_dag(dag_id=DAG_ID)
    
    merge_task = dag.get_task("load_daily_tables.merge_call_logs")
    sql = merge_task.sql.upper()
    
    assert "MERGE INTO" in sql
    assert "WHEN MATCHED THEN" in sql
    assert "WHEN NOT MATCHED THEN" in sql
    assert "CURRENT_TIMESTAMP()" in sql
    
    assert "DELETE FROM" not in sql 

def test_daily_load_copy_configuration(dagbag):
    """
    Verify that Daily Tables DO NOT use FORCE=TRUE (relying on Load History).
    """
    dag = dagbag.get_dag(dag_id=DAG_ID)
    copy_task = dag.get_task("load_daily_tables.copy_call_logs")
    
    copy_options = copy_task.copy_options.upper()
    assert "FORCE" not in copy_options