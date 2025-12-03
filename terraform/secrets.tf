# PostgreSQL DB Credentials
resource "aws_secretsmanager_secret" "postgres_creds" {
  name                    = "${var.project_name}/postgres_credentials"
  description             = "PostgreSQL credentials for the data platform."
  recovery_window_in_days = 0
}

resource "aws_secretsmanager_secret_version" "postgres_creds_version" {
  secret_id = aws_secretsmanager_secret.postgres_creds.id
  secret_string = jsonencode({
    username = "postgres.zrhqaykelfqavlbggufd",
    password = var.postgres_db_password,
    host     = "aws-1-eu-west-1.pooler.supabase.com",
    port     = 6543,
    dbname   = "postgres"
  })
}

# Source AWS Credentials 
resource "aws_secretsmanager_secret" "source_aws_creds" {
  name                    = "${var.project_name}/source_aws_credentials"
  description             = "AWS Keys for external Source S3 access."
  recovery_window_in_days = 0
}

resource "aws_secretsmanager_secret_version" "source_aws_creds_version" {
  secret_id = aws_secretsmanager_secret.source_aws_creds.id
  secret_string = jsonencode({
    aws_access_key_id     = "AKIAU6VTTFBONYY36CHM",
    aws_secret_access_key = var.source_secret_access_key
  })
}

# Target AWS Credentials
resource "aws_secretsmanager_secret" "target_aws_creds" {
  name                    = "${var.project_name}/target_aws_credentials"
  description             = "AWS Keys for internal Data Lake S3 access."
  recovery_window_in_days = 0
}

resource "aws_secretsmanager_secret_version" "target_aws_creds_version" {
  secret_id = aws_secretsmanager_secret.target_aws_creds.id
  secret_string = jsonencode({
    aws_access_key_id     = "AKIAUALX3AQ4WKV5SZ6N",
    aws_secret_access_key = var.target_secret_access_key
  })
}

# Snowflake Credentials
resource "aws_secretsmanager_secret" "snowflake_creds" {
  name                    = "${var.project_name}/snowflake_credentials"
  description             = "Snowflake credentials for data warehousing (Target)."
  recovery_window_in_days = 0
}

resource "aws_secretsmanager_secret_version" "snowflake_creds_version" {
  secret_id = aws_secretsmanager_secret.snowflake_creds.id
  secret_string = jsonencode({
    account   = "rtrzzlx-br15917",
    user      = "DATAWITHKAY",
    password  = var.snowflake_password,
    role      = "ACCOUNTADMIN",
    warehouse = "CORETELECOMS_WH"
  })
}
