resource "aws_iam_role" "snowflake_role" {
  name = "${var.project_name}-snowflake-access-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          AWS = "arn:aws:iam::${data.aws_caller_identity.current.account_id}:root"
        }
        Action = "sts:AssumeRole"
      }
    ]
  })
}

resource "aws_iam_policy" "snowflake_s3_access" {
  name        = "${var.project_name}-snowflake-s3-policy"
  description = "Allow Snowflake to read from Raw Zone S3 Bucket"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow",
        Action = [
          "s3:GetObject",
          "s3:GetObjectVersion",
          "s3:ListBucket"
        ],
        Resource = [
          aws_s3_bucket.raw_zone.arn,
          "${aws_s3_bucket.raw_zone.arn}/*"
        ]
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "attach_snowflake_access" {
  role       = aws_iam_role.snowflake_role.name
  policy_arn = aws_iam_policy.snowflake_s3_access.arn
}

resource "snowflake_storage_integration" "s3_int" {
  name             = upper("${var.project_name}_S3_INT")
  comment          = "Integration for ${var.project_name} Raw Zone"
  type             = "EXTERNAL_STAGE"
  enabled          = true
  storage_provider = "S3"

  storage_aws_role_arn = aws_iam_role.snowflake_role.arn

  storage_allowed_locations = [
    "s3://${aws_s3_bucket.raw_zone.bucket}/"
  ]
}
