import pytest
from airflow.models import DagBag
import sys
import os

TEST_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(TEST_DIR)
DAGS_FOLDER = os.path.join(PROJECT_ROOT, "dags")
SCRIPTS_FOLDER = os.path.join(PROJECT_ROOT, "scripts")

sys.path.append(SCRIPTS_FOLDER)

@pytest.fixture
def dagbag(mock_airflow_variables):
    """
    Loads the DAGs from the specific folder. 
    Passed 'mock_airflow_variables' to ensure any Top-Level Code 
    """
    return DagBag(dag_folder=DAGS_FOLDER, include_examples=False)

def test_no_import_errors(dagbag):
    """
    Check for syntax errors or missing imports in DAG files.
    If this fails, it prints the specific python error causing the crash.
    """
    error_msg = "\n".join([f"{k}: {v}" for k, v in dagbag.import_errors.items()])
    assert len(dagbag.import_errors) == 0, f"DAG Import Errors found:\n{error_msg}"

def test_ingestion_dag_structure(dagbag):
    """
    Validates the specific configuration of the 'ingestion_pipeline' DAG.
    """
    dag_id = "ingestion_pipeline"
    
    assert dag_id in dagbag.dags, f"DAG '{dag_id}' not found in DagBag. Loaded DAGs: {dagbag.dags.keys()}"
    
    dag = dagbag.get_dag(dag_id)
    
    assert dag is not None
    assert dag.catchup is True
    assert dag.schedule == "@daily"
    
    task_ids = [t.task_id for t in dag.tasks]
    
    assert "daily_ingestion.extract_postgres" in task_ids, \
        f"Task 'extract_postgres' not found. Available tasks: {task_ids}"
    
    assert "static_ingestion.extract_agents_gsheet" in task_ids, \
        f"Task 'extract_agents_gsheet' not found. Available tasks: {task_ids}"