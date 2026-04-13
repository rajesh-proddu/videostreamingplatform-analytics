output "iceberg_warehouse_bucket" {
  description = "S3 bucket name for Iceberg warehouse"
  value       = aws_s3_bucket.iceberg_warehouse.bucket
}

output "iceberg_warehouse_arn" {
  description = "S3 bucket ARN for Iceberg warehouse"
  value       = aws_s3_bucket.iceberg_warehouse.arn
}

output "glue_database_name" {
  description = "AWS Glue catalog database name"
  value       = aws_glue_catalog_database.analytics.name
}

output "iceberg_readwrite_policy_arn" {
  description = "IAM policy ARN for Glue + S3 Iceberg access"
  value       = aws_iam_policy.iceberg_readwrite.arn
}
