import pytest
import os
import sys
import polars as pl
from unittest.mock import patch, MagicMock

from scripts.extract_s3_data import extract_s3_to_s3
from scripts.extract_gsheets import extract_gsheets_to_s3
from scripts.extract_postgres import extract_and_upload

# ---------------------------------------------------------
# S3 Extraction Tests
# ---------------------------------------------------------
def test_s3_call_logs_csv_processing(s3_setup):
    s3 = s3_setup
    source_bucket = "core-telecoms-data-lake"
    target_bucket = "target-bucket-dev"
    date = "2025-11-20"
    
    csv_content = "call ID,customeR iD,COMPLAINT_catego ry,agent ID,call_start_time\n1,CUST001,Billing,101,2025-01-01 10:00:00"
    s3.put_object(Bucket=source_bucket, Key=f"call_logs_day_{date}.csv", Body=csv_content)

    try:
        extract_s3_to_s3(date, source_bucket, "call_logs", target_bucket, "csv")
    except SystemExit as e:
        assert e.code == 0

    expected_key = f"raw/call_logs/{date}/data.parquet"
    objects = s3.list_objects(Bucket=target_bucket)
    keys = [o['Key'] for o in objects.get('Contents', [])]
    assert expected_key in keys

def test_s3_social_media_json_structure(s3_setup):
    s3 = s3_setup
    source_bucket = "core-telecoms-data-lake"
    target_bucket = "target-bucket-dev"
    date = "2025-11-20"
    
    json_content = b'{"complaint_id": {"0": "dan8e7e", "1": "jare853"}, "date": {"0": "2025-11-20", "1": "2025-11-20"}}'
    s3.put_object(Bucket=source_bucket, Key=f"media_complaint_day_{date}.json", Body=json_content)

    try:
        extract_s3_to_s3(date, source_bucket, "media_complaint", target_bucket, "json")
    except SystemExit as e:
        assert e.code == 0

    expected_key = f"raw/social_media/{date}/data.parquet"
    objects = s3.list_objects(Bucket=target_bucket)
    keys = [o['Key'] for o in objects.get('Contents', [])]
    assert expected_key in keys

# ---------------------------------------------------------
# Google Sheets Tests
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
# Postgres Tests
# ---------------------------------------------------------
@patch('extract_postgres.pl.read_database_uri')
@patch('extract_postgres.boto3.client')
@patch('os.getenv')
def test_postgres_logic(mock_env, mock_boto, mock_read_db):
    mock_env.side_effect = lambda k, d=None: "test_val" if k in ["POSTGRES_USER", "POSTGRES_PASSWORD", "POSTGRES_HOST", "POSTGRES_DB"] else d
    
    mock_df = pl.DataFrame({"col1": [1], "col2": ["a"]})
    mock_read_db.return_value = mock_df
    
    try:
        extract_and_upload("2025-11-20", "target-bucket")
    except SystemExit:
        pass

    args, _ = mock_read_db.call_args
    query = args[0]
    assert "Web_form_request_2025_11_20" in query