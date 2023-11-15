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
    default = "arn:aws:iam::177474864456:role/service-role/AWSGlueServiceRole"
}

variable "region" {
    type = string
    default = "eu-west-1"
    }