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