import pytest
import boto3
from moto import mock_aws
import os
import sys
from unittest.mock import patch

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../scripts')))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../dags')))

@pytest.fixture(scope="function")
def aws_credentials(monkeypatch):
    """
    Mocked AWS Credentials for moto. 
    """
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    monkeypatch.setenv("AWS_SECURITY_TOKEN", "testing")
    monkeypatch.setenv("AWS_SESSION_TOKEN", "testing")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "eu-north-1")
    
    monkeypatch.setenv("SOURCE_AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("SOURCE_AWS_SECRET_ACCESS_KEY", "testing")
    monkeypatch.setenv("SOURCE_AWS_REGION", "eu-north-1")

@pytest.fixture(scope="function")
def s3_setup(aws_credentials):
    """
    Creates Source and Target buckets in the Mock S3 environment.
    This runs BEFORE the tests to ensure buckets exist.
    """
    with mock_aws():
        s3 = boto3.client("s3", region_name="eu-north-1")
        
        s3.create_bucket(
            Bucket="core-telecoms-data-lake", 
            CreateBucketConfiguration={'LocationConstraint': 'eu-north-1'}
        )
        s3.create_bucket(
            Bucket="target-bucket-dev", 
            CreateBucketConfiguration={'LocationConstraint': 'eu-north-1'}
        )
        
        yield s3

@pytest.fixture(autouse=True)
def mock_airflow_variables():
    """
    Patches Airflow Variable.get to return the MOCK bucket names.
    This ensures that if the code asks Airflow for a bucket name, 
    it gets the one that exists in our Mock S3.
    """
    with patch("airflow.models.Variable.get") as mock_get:
        def side_effect(key, default_var=None):
            if key == "S3_RAW_BUCKET": return "target-bucket-dev"
            if key == "SOURCE_S3_BUCKET": return "core-telecoms-data-lake"
            return default_var
        
        mock_get.side_effect = side_effect
        yield mock_get