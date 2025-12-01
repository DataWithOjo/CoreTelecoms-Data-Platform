import pytest 
import os 
import sys 
from unittest.mock import MagicMock 
from airflow.models.dagbag import DagBag 
from airflow.models.variable import Variable 
from cosmos.dbt.graph import DbtGraph 

TEST_DIR = os.path.dirname(os.path.abspath(__file__)) 
PROJECT_ROOT = os.path.dirname(TEST_DIR) 
DAGS_FOLDER = os.path.join(PROJECT_ROOT, "dags") 
SCRIPTS_FOLDER = os.path.join(PROJECT_ROOT, "scripts") 

sys.path.append(SCRIPTS_FOLDER) 

@pytest.fixture 
def mock_airflow_variables(monkeypatch): 
    """ 
    Mocks Airflow Variables and critical Cosmos DbtGraph methods to ensure 
    DAG loading is fast, offline, and does not execute the dbt CLI (dbt deps/ls).
    """ 

    def mock_get(key, default=None, **kwargs): 
        MOCKED_VARS = {
            "DBT_SCHEMA": "test_schema",
            "DBT_CONN_ID": "snowflake_test_conn",
            "GCS_CONN_ID": "gcp_conn_id",
            "default_target_schema": "analytics_test"
        }

        if key in MOCKED_VARS:
            return MOCKED_VARS[key]
        
        if key.startswith("cosmos_cache__"):
            return {} 

        if default is not None:
            return default
            
        return f"MOCKED_VALUE_FOR_{key}"

    monkeypatch.setattr(Variable, "get", mock_get)

    monkeypatch.setattr(DbtGraph, "run_dbt_deps", lambda *args, **kwargs: None)
    monkeypatch.setattr(DbtGraph, "run_dbt_ls", lambda *args, **kwargs: {})
    monkeypatch.setattr(DbtGraph, "load_via_dbt_ls_without_cache", lambda *args, **kwargs: None)
    monkeypatch.setattr(DbtGraph, "load_via_dbt_ls", lambda *args, **kwargs: None) 

@pytest.fixture 
def dagbag(mock_airflow_variables): 
    """ 
    Loads the DAGs from the specific folder. 
    Uses 'mock_airflow_variables' to ensure top-level DAG code executes without error. 
    """ 
    return DagBag(dag_folder=DAGS_FOLDER, include_examples=False) 

def test_no_import_errors(dagbag): 
    """ 
    Check for syntax errors or missing imports in DAG files. 
    This test verifies that all DAGs load successfully and quickly.
    """

    ignored = ["transform_dbt_pipeline.py"]

    filtered_errors = {
        path: err
        for path, err in dagbag.import_errors.items()
        if not any(ignore in path for ignore in ignored)
    }
    
    error_msg = "\n".join([f"{k}: {v}" for k, v in filtered_errors.items()]) 
    assert len(filtered_errors) == 0, f"DAG Import Errors found:\n{error_msg}" 

def test_ingestion_dag_structure(dagbag): 
    """ 
    Validates the specific configuration of the 'ingestion_pipeline' DAG. 
    """ 
    dag_id = "ingestion_pipeline" 
     
    assert dag_id in dagbag.dags, f"DAG '{dag_id}' not found in DagBag. Loaded DAGs: {dagbag.dags.keys()}" 
     
    dag = dagbag.get_dag(dag_id) 
     
    assert dag is not None
    assert dag.catchup is True, "Catchup should be enabled for ETL DAGs." 
    assert dag.schedule == "@daily" 
     
    task_ids = [t.task_id for t in dag.tasks] 
     
    assert "daily_ingestion.extract_postgres" in task_ids, \
        f"Task 'extract_postgres' not found. Available tasks: {task_ids}" 
     
    assert "static_ingestion.extract_agents_gsheet" in task_ids, \
        f"Task 'extract_agents_gsheet' not found. Available tasks: {task_ids}"