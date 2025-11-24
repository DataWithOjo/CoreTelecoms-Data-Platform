import pytest
import boto3
from moto import mock_aws
import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../scripts')))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../dags')))

@pytest.fixture
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "eu-north-1"
    os.environ["SOURCE_AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["SOURCE_AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["SOURCE_AWS_REGION"] = "eu-north-1"

@pytest.fixture
def s3_setup(aws_credentials):
    """Creates Source and Target buckets in the Mock S3 environment."""
    with mock_aws():
        s3 = boto3.client("s3", region_name="eu-north-1")
        s3.create_bucket(Bucket="core-telecoms-data-lake", CreateBucketConfiguration={'LocationConstraint': 'eu-north-1'})
        s3.create_bucket(Bucket="target-bucket-dev", CreateBucketConfiguration={'LocationConstraint': 'eu-north-1'})
        yield s3

from unittest.mock import patch
@pytest.fixture(autouse=True)
def mock_airflow_variables():
    with patch("airflow.models.Variable.get") as mock_get:
        def side_effect(key, default_var=None):
            if key == "S3_RAW_BUCKET": return "test-bucket"
            if key == "SOURCE_S3_BUCKET": return "test-source-bucket"
            return default_var
        mock_get.side_effect = side_effect
        yield mock_get