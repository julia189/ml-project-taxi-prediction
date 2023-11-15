provider "aws" {
  region  = "eu-west-1"
  profile = var.profile
}

resource "aws_glue_job" "this" {
    name = "preprocessing-etl"
    role_arn = var.gluerole
    max_capacigty = 1
    glue_version = "4.0"

 default_arguments = {
 "--enable-job-insights" = "true",

 }

 command {
    name = "glueetl"
    script_location = ""
 }
}

