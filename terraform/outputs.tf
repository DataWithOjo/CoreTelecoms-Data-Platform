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