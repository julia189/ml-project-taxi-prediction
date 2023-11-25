
provider "aws" {
  region  = var.region
  profile = var.awsprofile
}

resource "aws_glue_crawler" "data-ingestion-crawler" {
  database_name = "default"
  name          = "data-ingestion-crawler"
  role          = var.gluerole
  s3_target {
    path = "s3://test-database-jh/data/ingestion_data/trips_partitioned/"
  }
}



