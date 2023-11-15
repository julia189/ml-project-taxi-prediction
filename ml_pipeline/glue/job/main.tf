provider "aws" {
  region  = var.region
  profile = var.profile
}

resource "aws_glue_job" "preprocessing-etl" {
    name = "preprocessing-etl"
    role_arn = var.gluerole
    max_capacity = 1
    glue_version = "4.0"

 default_arguments = {
 "--enable-job-insights" = "true",
 "--additional-python-modules" = ""
 }

 command {
    name = "glueetl"
     script_location = ""

 }
}
