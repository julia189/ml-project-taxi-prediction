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
 "--additional-python-modules" = "datetime==5.3",
 # aws --profile Terraform_Playground s3 cp ./src/preprocessing/dist/data_ingestion-1.0-py3-none-any.whl s3://etl-glue-job-scripts/scripts/
 "--extra-py-files" = "s3://etl-glue-job-scripts/scripts/data_ingestion-1.0-py3-none-any.whl",
 }

 command {
    name = "glueetl"
   # aws --profile Terraform_Playground s3 cp ./ml_pipeline/glue/job/preprocessing_etl.py s3://etl-glue-job-scripts/scripts/ 
    script_location = "s3://etl-glue-job-scripts/scripts/preprocessing_etl.py"
 }
}
