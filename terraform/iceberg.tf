# S3 bucket for Iceberg warehouse data
resource "aws_s3_bucket" "iceberg_warehouse" {
  bucket = "videostreamingplatform-iceberg-warehouse-${var.environment}"

  tags = {
    Service     = "analytics"
    Environment = var.environment
    ManagedBy   = "terraform"
  }
}

resource "aws_s3_bucket_versioning" "iceberg_warehouse" {
  bucket = aws_s3_bucket.iceberg_warehouse.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "iceberg_warehouse" {
  bucket = aws_s3_bucket.iceberg_warehouse.id

  rule {
    id     = "iceberg-cleanup"
    status = "Enabled"

    transition {
      days          = 90
      storage_class = "STANDARD_IA"
    }

    transition {
      days          = 365
      storage_class = "GLACIER"
    }

    abort_incomplete_multipart_upload {
      days_after_initiation = 7
    }
  }

  rule {
    id     = "expire-old-versions"
    status = "Enabled"

    noncurrent_version_expiration {
      noncurrent_days = 30
    }
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "iceberg_warehouse" {
  bucket = aws_s3_bucket.iceberg_warehouse.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "aws:kms"
    }
    bucket_key_enabled = true
  }
}

resource "aws_s3_bucket_public_access_block" "iceberg_warehouse" {
  bucket = aws_s3_bucket.iceberg_warehouse.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# ── AWS Glue Catalog ──────────────────────────────────────────────

resource "aws_glue_catalog_database" "analytics" {
  name = "analytics"

  description = "Iceberg catalog database for watch history analytics"
}

resource "aws_glue_catalog_table" "watch_history" {
  database_name = aws_glue_catalog_database.analytics.name
  name          = "watch_history"

  table_type = "EXTERNAL_TABLE"

  parameters = {
    "table_type"                        = "ICEBERG"
    "metadata_location"                 = "s3://${aws_s3_bucket.iceberg_warehouse.bucket}/analytics/watch_history/metadata/"
    "write.format.default"              = "parquet"
    "write.parquet.compression-codec"   = "zstd"
    "write.target-file-size-bytes"      = "134217728"
    "history.expire.max-snapshot-age-ms" = "432000000"
  }

  storage_descriptor {
    location      = "s3://${aws_s3_bucket.iceberg_warehouse.bucket}/analytics/watch_history/"
    input_format  = "org.apache.hadoop.mapred.FileInputFormat"
    output_format = "org.apache.hadoop.mapred.FileOutputFormat"

    columns {
      name = "event_type"
      type = "string"
    }
    columns {
      name = "video_id"
      type = "string"
    }
    columns {
      name = "user_id"
      type = "string"
    }
    columns {
      name = "session_id"
      type = "string"
    }
    columns {
      name = "bytes_read"
      type = "bigint"
    }
    columns {
      name = "event_ts"
      type = "timestamp"
    }
    columns {
      name = "ingested_at"
      type = "timestamp"
    }
  }

  partition_keys {
    name = "event_day"
    type = "date"
  }
}

# ── IAM for Glue + S3 access ─────────────────────────────────────

resource "aws_iam_policy" "iceberg_readwrite" {
  name        = "iceberg-warehouse-readwrite-${var.environment}"
  description = "Glue catalog + S3 warehouse access for Iceberg consumers"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "GlueCatalogAccess"
        Effect = "Allow"
        Action = [
          "glue:GetDatabase",
          "glue:GetDatabases",
          "glue:GetTable",
          "glue:GetTables",
          "glue:CreateTable",
          "glue:UpdateTable",
          "glue:DeleteTable",
          "glue:GetPartitions",
          "glue:BatchGetPartition",
          "glue:CreatePartition",
          "glue:BatchCreatePartition",
          "glue:UpdatePartition",
          "glue:DeletePartition",
        ]
        Resource = [
          "arn:aws:glue:${var.aws_region}:*:catalog",
          "arn:aws:glue:${var.aws_region}:*:database/${aws_glue_catalog_database.analytics.name}",
          "arn:aws:glue:${var.aws_region}:*:table/${aws_glue_catalog_database.analytics.name}/*",
        ]
      },
      {
        Sid    = "IcebergBucketAccess"
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:ListBucket",
          "s3:GetBucketLocation",
        ]
        Resource = [
          aws_s3_bucket.iceberg_warehouse.arn,
          "${aws_s3_bucket.iceberg_warehouse.arn}/*",
        ]
      },
    ]
  })
}
