import pytest
import os
import sys
import polars as pl
from unittest.mock import patch, MagicMock
from extract_s3_data import extract_s3_to_s3
from extract_gsheets import extract_gsheets_to_s3
from extract_postgres import extract_and_upload

# ---------------------------------------------------------
# S3 Extraction Tests
# ---------------------------------------------------------

def test_s3_call_logs_csv_success(s3_setup):
    """Test successful extraction of Call Logs CSV."""
    s3 = s3_setup
    source_bucket = "core-telecoms-data-lake"
    target_bucket = "target-bucket-dev"
    date = "2025-11-20"
    
    source_key = f"call logs/call_logs_day_{date}.csv"
    
    csv_content = b"call ID,customeR iD,COMPLAINT_catego ry,agent ID,call_start_time\n1,CUST001,Billing,101,2025-01-01 10:00:00"
    s3.put_object(Bucket=source_bucket, Key=source_key, Body=csv_content)

    extract_s3_to_s3(date, source_bucket, "call_logs", target_bucket, "csv")

    expected_key = f"raw/call_logs/{date}/data.parquet"
    objects = s3.list_objects(Bucket=target_bucket)
    keys = [o['Key'] for o in objects.get('Contents', [])]
    assert expected_key in keys

def test_s3_missing_file_skips(s3_setup):
    """Test that missing files trigger SystemExit(99) skip instead of failure."""
    source_bucket = "core-telecoms-data-lake"
    target_bucket = "target-bucket-dev"
    date = "2025-11-25"

    with pytest.raises(SystemExit) as e:
        extract_s3_to_s3(date, source_bucket, "call_logs", target_bucket, "csv")
    
    assert e.value.code == 99

def test_s3_social_media_json_structure(s3_setup):
    s3 = s3_setup
    source_bucket = "core-telecoms-data-lake"
    target_bucket = "target-bucket-dev"
    date = "2025-11-20"
    
    source_key = f"social_medias/media_complaint_day_{date}.json"

    json_content = b'{"complaint_id": {"0": "dan8e7e", "1": "jare853"}, "date": {"0": "2025-11-20", "1": "2025-11-20"}}'
    s3.put_object(Bucket=source_bucket, Key=source_key, Body=json_content)

    extract_s3_to_s3(date, source_bucket, "media_complaint", target_bucket, "json")

    expected_key = f"raw/social_media/{date}/data.parquet"
    objects = s3.list_objects(Bucket=target_bucket)
    keys = [o['Key'] for o in objects.get('Contents', [])]
    assert expected_key in keys

# ---------------------------------------------------------
# Google Extraction Tests
# ---------------------------------------------------------

@patch('extract_gsheets.gspread.authorize')
@patch('extract_gsheets.ServiceAccountCredentials.from_json_keyfile_name')
@patch('extract_gsheets.boto3.client')
@patch('os.path.exists')
def test_gsheets_logic(mock_exists, mock_boto, mock_creds, mock_auth):
    mock_exists.return_value = True
    mock_client = MagicMock()
    mock_spreadsheet = MagicMock()
    mock_sheet = MagicMock()
    
    mock_auth.return_value = mock_client
    mock_client.open.return_value = mock_spreadsheet
    mock_spreadsheet.sheet1 = mock_sheet
    
    mock_data = [{"iD": 1000, "NamE": "Allison", "experience": "Mid", "state": "RI"}]
    mock_sheet.get_all_records.return_value = mock_data
    
    mock_s3 = MagicMock()
    mock_boto.return_value = mock_s3
    
    try:
        extract_gsheets_to_s3("target-bucket")
    except SystemExit:
        pass
        
    mock_s3.upload_file.assert_called_once()

# ---------------------------------------------------------
# Postgres Extraction Tests
# ---------------------------------------------------------

@patch('extract_postgres.create_engine')
@patch('extract_postgres.pl.read_database')
@patch('extract_postgres.boto3.client')
@patch('os.environ')
def test_postgres_logic(mock_env, mock_boto, mock_read_db, mock_create_engine):

    env_vars = {
        "POSTGRES_USER": "user",
        "POSTGRES_PASSWORD": "password",
        "POSTGRES_HOST": "localhost",
        "POSTGRES_DB": "db"
    }
    mock_env.__getitem__.side_effect = env_vars.__getitem__
    mock_env.get.side_effect = env_vars.get


    mock_engine = MagicMock()
    mock_connection = MagicMock()
    mock_create_engine.return_value = mock_engine

    mock_engine.connect.return_value.__enter__.return_value = mock_connection

    mock_df = pl.DataFrame({"col1": [1], "col2": ["a"]})
    mock_read_db.return_value = mock_df

    mock_s3 = MagicMock()
    mock_boto.return_value = mock_s3

    extract_and_upload("2025-11-20", "target-bucket")

    assert mock_read_db.called
    call_kwargs = mock_read_db.call_args[1]
    assert call_kwargs['connection'] == mock_connection
    
    call_args = mock_read_db.call_args[1]
    assert "Web_form_request_2025_11_20" in call_kwargs['query']

    mock_s3.upload_file.assert_called_once()