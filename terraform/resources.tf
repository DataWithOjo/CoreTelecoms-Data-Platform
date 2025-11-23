resource "aws_s3_bucket" "raw_zone" {
  bucket        = "${var.project_name}-raw-zone-oluwakayode-dev"
  force_destroy = true
}

resource "aws_s3_bucket_versioning" "raw_ver" {
  bucket = aws_s3_bucket.raw_zone.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_ecr_repository" "airflow_repo" {
  name                 = "${var.project_name}-airflow"
  image_tag_mutability = "MUTABLE"

  image_scanning_configuration {
    scan_on_push = true
  }
}

resource "aws_secretsmanager_secret" "db_creds" {
  name        = "${var.project_name}/postgres_creds"
  description = "Credentials for Source Postgres DB"
}

resource "aws_secretsmanager_secret" "google_creds" {
  name        = "${var.project_name}/google_service_account"
  description = "JSON Key for Google Sheets Access"
}

resource "snowflake_database" "dw" {
  name = upper("${var.project_name}_DW")
}

resource "snowflake_warehouse" "wh" {
  name           = "CORETELECOMS_WH"
  warehouse_size = "x-small"
  auto_suspend   = 60
}

resource "snowflake_schema" "raw_schema" {
  database = snowflake_database.dw.name
  name     = "RAW"
}

resource "snowflake_schema" "analytics_schema" {
  database = snowflake_database.dw.name
  name     = "ANALYTICS"
}