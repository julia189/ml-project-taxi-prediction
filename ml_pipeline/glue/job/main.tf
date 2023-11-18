provider "aws" {
  region  = var.region
  profile = var.awsprofile
}

resource "aws_glue_job" "preprocessing-etl" {
    name = "preprocessing-etl"
    role_arn = var.gluerole
    max_capacity = 1
    glue_version = "4.0"

 default_arguments = {
 "--enable-job-insights" = "true",
 "--additional-python-modules" = "logging==0.4.9.6,datetime==5.3",
 "--extra-py-files" = "",
 }

 command {
    name = "glueetl"
    # update file: aws s3 cp /Users/Q619505/PycharmProjects/ml-project-taxi-prediction/ml_pipeline/glue/job/preprocessing_etl.py 
    script_location = "ml_pipeline/glue/job/preprocessing_etl.py"
 }
}
