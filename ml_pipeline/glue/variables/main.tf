variable "awsprofile" {
    type = string
    default = "Terraform_Playground"
}

variable "gluerole" {
    type = string
    default = "arn:aws:iam::177474864456:role/service-role/AWSGlueServiceRole"
}
