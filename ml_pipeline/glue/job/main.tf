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
 #"--additional-python-modules" = "awswrangler==3.4.2" #"s3://etl-glue-job-scripts/scripts/dataingestion-1.0-py3-none-any.whl,datetime==5.3",
 # aws --profile Terraform_Playground s3 cp ./src/preprocessing/dist/dataingestion-1.0-py3-none-any.whl s3://etl-glue-job-scripts/scripts/
 # "--extra-py-files" = "s3://etl-glue-job-scripts/scripts/dataingestion-1.0-py3-none-any.whl",
 #"--python-modules-installer-option" = "--upgrade"
 }

 command {
    name = "glueetl"
   # aws --profile Terraform_Playground s3 cp ./ml_pipeline/glue/job/preprocessing_etl.py s3://etl-glue-job-scripts/scripts/ 
    script_location = "s3://etl-glue-job-scripts/scripts/preprocessing_etl.py"
 }
}
