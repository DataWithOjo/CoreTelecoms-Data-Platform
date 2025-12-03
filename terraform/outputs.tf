output "s3_raw_bucket" {
  description = "Name of the Raw Data Lake bucket"
  value       = aws_s3_bucket.raw_zone.id
}

output "ecr_repository_url" {
  description = "URL of the ECR repo to push Docker images to"
  value       = aws_ecr_repository.airflow_repo.repository_url
}

output "snowflake_database" {
  value = snowflake_database.dw.name
}

output "snowflake_iam_user_arn" {
  value       = snowflake_storage_integration.s3_int.storage_aws_iam_user_arn
  description = "The Snowflake User ARN to paste into AWS IAM Trust Policy"
}

output "snowflake_external_id" {
  value       = snowflake_storage_integration.s3_int.storage_aws_external_id
  description = "The Snowflake External ID to paste into AWS IAM Trust Policy"
}