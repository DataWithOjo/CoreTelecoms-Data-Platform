import pytest
from airflow.models import DagBag
from airflow.utils.dag_cycle_tester import check_cycle

DAG_ID = "load_snowflake_pipeline"

@pytest.fixture()
def dagbag():
    """Create a DagBag instance to load DAGs from the file."""
    return DagBag(dag_folder="airflow/dags/load_snowflake_pipeline.py", include_examples=False)

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
        "load_static_tables.load_customers",
        "load_static_tables.load_agents",
        "load_daily_tables.load_call_logs",
        "load_daily_tables.load_web_complaints",
        "load_daily_tables.load_social_media",
        "send_success_email"
    ]
    
    for task_id in expected_tasks:
        assert task_id in task_ids, f"Task {task_id} missing from DAG"

def test_no_cycles(dagbag):
    """Ensure the DAG has no circular dependencies."""
    dag = dagbag.get_dag(dag_id=DAG_ID)
    check_cycle(dag)

def test_static_load_sql_logic(dagbag):
    """
    Verify that Static Tables use Transaction Safety and FORCE=TRUE.
    """
    dag = dagbag.get_dag(dag_id=DAG_ID)
    customer_task = dag.get_task("load_static_tables.load_customers")
    
    sql = customer_task.sql.upper()
    
    assert "BEGIN TRANSACTION" in sql
    assert "DELETE FROM" in sql
    assert "COMMIT" in sql
    assert "ROLLBACK" in sql
    
    assert "FORCE = TRUE" in sql
    assert "ON_ERROR = SKIP_FILE" in sql

def test_daily_load_sql_logic(dagbag):
    """
    Verify that Daily Tables use Append logic and capture timestamps.
    """
    dag = dagbag.get_dag(dag_id=DAG_ID)
    logs_task = dag.get_task("load_daily_tables.load_call_logs")
    
    sql = logs_task.sql.upper()
    
    assert "TRUNCATE" not in sql
    assert "COPY INTO" in sql
    
    assert "CURRENT_TIMESTAMP()" in sql