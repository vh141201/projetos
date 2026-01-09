provider "aws" {
  region = "us-east-1"
}

resource "aws_s3_bucket" "finance_raw" {
  bucket = "data-batch-cripto-api-raw-victorhvm-2026" 
}

resource "aws_s3_bucket" "athena_results" {
  bucket = "athena-results-victorhvm-2026" 
}

resource "aws_athena_workgroup" "crypto_analysis" {
  name = "crypto_analysis_workgroup"

  configuration {
    result_configuration {
      output_location = "s3://${aws_s3_bucket.athena_results.bucket}/results/"
    }
  }

  force_destroy = true
}

resource "aws_glue_catalog_database" "crypto_db" {
  name = "crypto_analytics_db"
}

resource "aws_glue_catalog_table" "bitcoin_table" {
  name          = "bitcoin_prices"
  database_name = aws_glue_catalog_database.crypto_db.name
  table_type    = "EXTERNAL_TABLE"

  partition_keys {
    name = "coin"
    type = "string"
  }

  parameters = {
    "classification" = "parquet"
  }

  storage_descriptor {
    location      = "s3://${aws_s3_bucket.finance_raw.bucket}/silver/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

    ser_de_info {
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
    }

    columns {
      name = "usd"
      type = "double"
    }
    columns {
      name = "timestamp"
      type = "timestamp"
    }
  }
}

resource "aws_glue_catalog_table" "crypto_gold_table" {
  name          = "crypto_daily_summary"
  database_name = aws_glue_catalog_database.crypto_db.name
  table_type    = "EXTERNAL_TABLE"

  parameters = {
    "classification" = "parquet"
  }

  storage_descriptor {
    location      = "s3://${aws_s3_bucket.finance_raw.bucket}/gold/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

    ser_de_info {
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
    }

    columns {
      name = "coin"
      type = "string"
    }
    columns {
      name = "price_mean"
      type = "double"
    }
    columns {
      name = "price_min"
      type = "double"
    }
    columns {
      name = "price_max"
      type = "double"
    }
    columns {
      name = "last_update"
      type = "timestamp"
    }
  }
}