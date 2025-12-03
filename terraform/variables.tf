variable "aws_region" {
  description = "AWS Region"
  default     = "eu-north-1"
}

variable "project_name" {
  description = "Project name to tag resources with"
  type        = string
  default     = "coretelecoms"
}

variable "snowflake_org" {
  description = "Snowflake Organization Name"
  type        = string
  sensitive   = true
}

variable "snowflake_account_name" {
  description = "Snowflake Account Name"
  type        = string
  sensitive   = true
}

variable "snowflake_user" {
  description = "Snowflake Username"
  type        = string
  sensitive   = true
}

variable "snowflake_password" {
  description = "Snowflake Password"
  type        = string
  sensitive   = true
}

variable "source_secret_access_key" {
  description = "The secret access key for the Source AWS Account."
  type        = string
  sensitive   = true
}

variable "target_secret_access_key" {
  description = "The secret access key for the Target Bucket AWS Account."
  type        = string
  sensitive   = true
}

variable "postgres_db_password" {
  description = "Password for the CoreTelecoms PostgreSQL database."
  type        = string
  sensitive   = true
}

variable "developer_external_id" {
  description = "A unique identifier required for local developers to assume the ETL role."
  type        = string
  default     = "coretelecoms-dev-local"
}