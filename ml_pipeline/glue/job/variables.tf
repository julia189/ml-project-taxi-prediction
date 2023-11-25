variable "awsprofile" {
    type = string
    default = "Terraform_Playground"
}

variable "crawlername" {
    type = string
    default = "raw-data-crawler"
}

variable "jobname" {
    type = string
    default = "raw-data-glue-job"
}

variable "gluerole" {
    type = string
    default = "arn:aws:iam::342774253254:role/Glue_Execution_Role"
}

variable "region" {
    type = string
    default = "eu-west-1"
    }