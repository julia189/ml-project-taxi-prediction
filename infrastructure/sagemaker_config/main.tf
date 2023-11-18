provider "aws" {
  region  = var.region
  profile = var.awsprofile
}

resource "aws_sagemaker_notebook_instance_lifecycle_configuration" "lc" {
  name      = "sm-notebook-autostop"
  on_start  = base64encode("./ml-project-taxi-prediction/scripts/sagemaker_instance/sm-autostop.sh")
}
