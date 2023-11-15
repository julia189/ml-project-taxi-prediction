variable "awsprofile" {
    type = string
    default = "Terraform_Playground"
}

variable "crawlername" {
    type = string
    default = "raw-data-crawler"
}

variable "gluerole" {
    type = string

    default = "arn:aws:iam::342774253254:role/service-role/AWSGlueServiceRole"
}

variable "region" {
    type = string
    default = "eu-west-1"
    }